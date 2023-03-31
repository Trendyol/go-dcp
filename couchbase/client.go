package couchbase

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/json-iterator/go"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/models"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

type Client interface {
	Ping() error
	GetAgent() *gocbcore.Agent
	Connect() error
	Close()
	DcpConnect() error
	DcpClose()
	GetBucketUUID() string
	GetVBucketSeqNos() (map[uint16]uint64, error)
	GetNumVBuckets() int
	GetVBucketUUIDMap(vbIds []uint16) (map[uint16]gocbcore.VbUUID, error)
	OpenStream(
		vbID uint16,
		vbUUID gocbcore.VbUUID,
		collectionIDs map[uint32]string,
		offset *models.Offset,
		observer Observer,
	) error
	CloseStream(vbID uint16) error
	GetCollectionIDs(scopeName string, collectionNames []string) (map[uint32]string, error)
	UpsertXattrs(ctx context.Context, scopeName string, collectionName string, id []byte, path string, xattrs interface{}, expiry uint32) error
	CreateDocument(ctx context.Context, scopeName string, collectionName string, id []byte, value interface{}, expiry uint32) error
	UpdateDocument(ctx context.Context, scopeName string, collectionName string, id []byte, value interface{}, expiry uint32) error
	DeleteDocument(ctx context.Context, scopeName string, collectionName string, id []byte)
	GetXattrs(scopeName string, collectionName string, id []byte, path string) ([]byte, error)
	CreatePath(ctx context.Context, scopeName string, collectionName string, id []byte, path []byte, value interface{}) error
	Get(ctx context.Context, scopeName string, collectionName string, id []byte) ([]byte, error)
}

type client struct {
	agent     *gocbcore.Agent
	metaAgent *gocbcore.Agent
	dcpAgent  *gocbcore.DCPAgent
	config    *helpers.Config
}

func (s *client) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.HealthCheck.Timeout)
	defer cancel()

	opm := NewAsyncOp(ctx)

	errorCh := make(chan error)

	op, err := s.agent.Ping(gocbcore.PingOptions{
		ServiceTypes: []gocbcore.ServiceType{gocbcore.MemdService, gocbcore.MgmtService},
	}, func(result *gocbcore.PingResult, err error) {
		memdSuccess := false
		mgmtSuccess := false

		if err == nil {
			if memdServiceResults, ok := result.Services[gocbcore.MemdService]; ok {
				for _, memdServiceResult := range memdServiceResults {
					if memdServiceResult.Error == nil && memdServiceResult.State == gocbcore.PingStateOK {
						memdSuccess = true
						break
					}
				}
			}

			if mgmtServiceResults, ok := result.Services[gocbcore.MgmtService]; ok {
				for _, mgmtServiceResult := range mgmtServiceResults {
					if mgmtServiceResult.Error == nil && mgmtServiceResult.State == gocbcore.PingStateOK {
						mgmtSuccess = true
						break
					}
				}
			}
		}

		if !memdSuccess || !mgmtSuccess {
			if err == nil {
				err = errors.New("some services are not healthy")
			}
		}

		opm.Resolve()

		errorCh <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return err
	}

	return <-errorCh
}

func (s *client) GetAgent() *gocbcore.Agent {
	return s.agent
}

func (s *client) tlsRootCaProvider() *x509.CertPool {
	cert, err := os.ReadFile(os.ExpandEnv(s.config.RootCAPath))
	if err != nil {
		logger.ErrorLog.Printf("error while reading cert file: %v", err)
		panic(err)
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(cert)

	return nil
}

func (s *client) getSecurityConfig() gocbcore.SecurityConfig {
	config := gocbcore.SecurityConfig{
		Auth: gocbcore.PasswordAuthProvider{
			Username: s.config.Username,
			Password: s.config.Password,
		},
	}

	if s.config.SecureConnection {
		config.UseTLS = true
		config.TLSRootCAProvider = s.tlsRootCaProvider
	}

	return config
}

func (s *client) connect(bucketName string, connectionBufferSize uint) (*gocbcore.Agent, error) {
	client, err := gocbcore.CreateAgent(
		&gocbcore.AgentConfig{
			BucketName: bucketName,
			SeedConfig: gocbcore.SeedConfig{
				HTTPAddrs: s.config.Hosts,
			},
			SecurityConfig: s.getSecurityConfig(),
			CompressionConfig: gocbcore.CompressionConfig{
				Enabled: true,
			},
			IoConfig: gocbcore.IoConfig{
				UseCollections: true,
			},
			KVConfig: gocbcore.KVConfig{
				ConnectionBufferSize: connectionBufferSize,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	ch := make(chan error)

	_, err = client.WaitUntilReady(
		time.Now().Add(s.config.Dcp.ConnectionTimeout),
		gocbcore.WaitUntilReadyOptions{
			RetryStrategy: gocbcore.NewBestEffortRetryStrategy(nil),
		},
		func(result *gocbcore.WaitUntilReadyResult, err error) {
			ch <- err
		},
	)

	if err != nil {
		return nil, err
	}

	if err = <-ch; err != nil {
		return nil, err
	}

	return client, nil
}

func (s *client) Connect() error {
	connectionBufferSize := s.config.Dcp.ConnectionBufferSize

	if s.config.IsCouchbaseMetadata() {
		metadataBucketName, _, _, metadataConnectionBufferSize := s.config.GetCouchbaseMetadata()
		if metadataBucketName == s.config.BucketName {
			if metadataConnectionBufferSize > connectionBufferSize {
				connectionBufferSize = metadataConnectionBufferSize
			}
		}
	}

	agent, err := s.connect(s.config.BucketName, connectionBufferSize)
	if err != nil {
		return err
	}

	s.agent = agent

	if s.config.IsCouchbaseMetadata() {
		metadataBucketName, _, _, connectionBufferSize := s.config.GetCouchbaseMetadata()

		if metadataBucketName == s.config.BucketName {
			s.metaAgent = agent
		} else {
			metaAgent, err := s.connect(metadataBucketName, connectionBufferSize)
			if err != nil {
				return err
			}

			s.metaAgent = metaAgent
		}

		logger.Log.Printf("connected to %s, bucket: %s, meta bucket: %s", s.config.Hosts, s.config.BucketName, metadataBucketName)
		return nil
	}

	logger.Log.Printf("connected to %s, bucket: %s", s.config.Hosts, s.config.BucketName)

	return nil
}

func (s *client) Close() {
	if s.metaAgent != nil {
		_ = s.metaAgent.Close()
	}

	if s.metaAgent != s.agent {
		_ = s.agent.Close()
	}

	logger.Log.Printf("connections closed %s", s.config.Hosts)
}

func (s *client) DcpConnect() error {
	agentConfig := &gocbcore.DCPAgentConfig{
		BucketName: s.config.BucketName,
		SeedConfig: gocbcore.SeedConfig{
			HTTPAddrs: s.config.Hosts,
		},
		SecurityConfig: s.getSecurityConfig(),
		CompressionConfig: gocbcore.CompressionConfig{
			Enabled: true,
		},
		DCPConfig: gocbcore.DCPConfig{
			BufferSize:      s.config.Dcp.BufferSize,
			UseExpiryOpcode: true,
		},
		KVConfig: gocbcore.KVConfig{
			ConnectionBufferSize: s.config.Dcp.ConnectionBufferSize,
		},
	}

	if s.config.IsCollectionModeEnabled() {
		agentConfig.IoConfig = gocbcore.IoConfig{
			UseCollections: true,
		}
	}

	client, err := gocbcore.CreateDcpAgent(
		agentConfig,
		fmt.Sprintf("%s_%s", s.config.Dcp.Group.Name, uuid.New().String()),
		memd.DcpOpenFlagProducer,
	)
	if err != nil {
		return err
	}

	ch := make(chan error)

	_, err = client.WaitUntilReady(
		time.Now().Add(time.Second*5),
		gocbcore.WaitUntilReadyOptions{
			RetryStrategy: gocbcore.NewBestEffortRetryStrategy(nil),
		},
		func(result *gocbcore.WaitUntilReadyResult, err error) {
			ch <- err
		},
	)

	if err != nil {
		return err
	}

	if err = <-ch; err != nil {
		return err
	}

	s.dcpAgent = client
	logger.Log.Printf("connected to %s as dcp, bucket: %s", s.config.Hosts, s.config.BucketName)

	return nil
}

func (s *client) DcpClose() {
	_ = s.dcpAgent.Close()
	logger.Log.Printf("dcp connection closed %s", s.config.Hosts)
}

func (s *client) GetBucketUUID() string {
	snapshot, err := s.dcpAgent.ConfigSnapshot()

	if err == nil {
		return snapshot.BucketUUID()
	}

	return ""
}

func (s *client) GetVBucketSeqNos() (map[uint16]uint64, error) {
	snapshot, err := s.dcpAgent.ConfigSnapshot()
	if err != nil {
		return nil, err
	}

	numNodes, err := snapshot.NumServers()
	if err != nil {
		return nil, err
	}

	seqNos := make(map[uint16]uint64)

	for i := 1; i <= numNodes; i++ {
		opm := NewAsyncOp(context.Background())

		op, err := s.dcpAgent.GetVbucketSeqnos(
			i,
			memd.VbucketStateActive,
			gocbcore.GetVbucketSeqnoOptions{},
			func(entries []gocbcore.VbSeqNoEntry, err error) {
				for _, en := range entries {
					seqNos[en.VbID] = uint64(en.SeqNo)
				}

				opm.Resolve()
			},
		)
		if err != nil {
			return nil, err
		}

		_ = opm.Wait(op, err)
	}

	return seqNos, nil
}

func (s *client) GetNumVBuckets() int {
	var err error

	if snapshot, err := s.dcpAgent.ConfigSnapshot(); err == nil {
		if vBuckets, err := snapshot.NumVbuckets(); err == nil {
			return vBuckets
		}
	}

	logger.ErrorLog.Printf("failed to get number of vbucket: %v", err)
	panic(err)
}

func (s *client) getNumReplicas() int {
	var err error

	if snapshot, err := s.dcpAgent.ConfigSnapshot(); err == nil {
		if replicas, err := snapshot.NumReplicas(); err == nil {
			return replicas
		}
	}

	logger.ErrorLog.Printf("failed to get number of replica: %v", err)
	panic(err)
}

func (s *client) getConfigSnapshot() *gocbcore.ConfigSnapshot {
	var err error

	if snapshot, err := s.dcpAgent.ConfigSnapshot(); err == nil {
		return snapshot
	}

	logger.ErrorLog.Printf("failed to get number of replica: %v", err)
	panic(err)
}

func (s *client) GetVBucketUUIDMap(vbIds []uint16) (map[uint16]gocbcore.VbUUID, error) {
	uuIDMap := make(map[uint16]gocbcore.VbUUID)

	for _, vbID := range vbIds {
		opm := NewAsyncOp(context.Background())

		op, err := s.dcpAgent.GetFailoverLog(
			vbID,
			func(entries []gocbcore.FailoverEntry, err error) {
				uuIDMap[vbID] = entries[0].VbUUID

				opm.Resolve()
			})
		if err != nil {
			return nil, err
		}

		_ = opm.Wait(op, err)
	}

	return uuIDMap, nil
}

func (s *client) getAbsentInstances(vbID uint16, replicas int) (map[int]bool, error) {
	snapshot := s.getConfigSnapshot()
	absentInstances := map[int]bool{}

	for idx := 0; idx <= replicas; idx++ {
		serverIndex, err := snapshot.VbucketToServer(vbID, uint32(idx))
		if err != nil {
			return nil, err
		}

		if serverIndex < 0 {
			absentInstances[idx] = true
		}
	}

	return absentInstances, nil
}

func (s *client) getObserveInstances(vbID uint16, vbUUID gocbcore.VbUUID, replicas int) (map[int]*gocbcore.ObserveVbResult, error) {
	observeInstances := map[int]*gocbcore.ObserveVbResult{}

	for idx := 0; idx <= replicas; idx++ {
		observeResult, err := s.observeVbID(vbID, vbUUID, idx)
		if err != nil {
			return nil, err
		}

		observeInstances[idx] = observeResult
	}

	return observeInstances, nil
}

func (s *client) getMinSeqNo(vbID uint16, vbUUID gocbcore.VbUUID) (gocbcore.SeqNo, error) {
	replicas := s.getNumReplicas()
	absentInstances, err := s.getAbsentInstances(vbID, replicas)
	if err != nil {
		return 0, err
	}

	startIndex := -1

	for i := 0; i <= replicas; i++ {
		if !absentInstances[i] {
			startIndex = i
			break
		}
	}

	if startIndex == -1 {
		return 0, nil
	}

	observeInstances, err := s.getObserveInstances(vbID, vbUUID, replicas)
	if err != nil {
		return 0, err
	}

	vbuuid := observeInstances[startIndex].VbUUID
	minSeqNo := observeInstances[startIndex].PersistSeqNo

	for i := startIndex + 1; i <= replicas; i++ {
		if absentInstances[i] {
			continue
		}

		if vbuuid != observeInstances[i].VbUUID {
			return 0, nil
		}

		if minSeqNo > observeInstances[i].PersistSeqNo {
			minSeqNo = observeInstances[i].PersistSeqNo
		}
	}

	return minSeqNo, nil
}

func (s *client) openStreamWithRollback(vbID uint16,
	vbUUID gocbcore.VbUUID,
	failedSeqNo gocbcore.SeqNo,
	observer Observer,
	openStreamOptions gocbcore.OpenStreamOptions,
) error {
	persistSeqNo, err := s.getMinSeqNo(vbID, vbUUID)
	if err != nil {
		return err
	}

	rollbackSeqNo := persistSeqNo

	if persistSeqNo > failedSeqNo {
		rollbackSeqNo = 0
	}

	if (persistSeqNo != 0 && failedSeqNo > persistSeqNo) ||
		persistSeqNo > failedSeqNo {
		observer.AddCatchup(vbID, uint64(failedSeqNo))
	}

	logger.Log.Printf(
		"open stream with rollback, vbID: %d, vbUUID: %d, failedSeqNo: %d, persistReqNo: %d, rollbackSeqNo: %d, catchup lag: %d",
		vbID, vbUUID,
		failedSeqNo, persistSeqNo, rollbackSeqNo, failedSeqNo-rollbackSeqNo,
	)

	opm := NewAsyncOp(context.Background())

	ch := make(chan error)

	op, err := s.dcpAgent.OpenStream(
		vbID,
		0,
		vbUUID,
		rollbackSeqNo,
		0xffffffffffffffff,
		rollbackSeqNo,
		rollbackSeqNo,
		observer,
		openStreamOptions,
		func(_ []gocbcore.FailoverEntry, err error) {
			opm.Resolve()

			ch <- err
		},
	)

	err = opm.Wait(op, err)
	if err != nil {
		return err
	}

	return <-ch
}

func (s *client) OpenStream(
	vbID uint16,
	vbUUID gocbcore.VbUUID,
	collectionIDs map[uint32]string,
	offset *models.Offset,
	observer Observer,
) error {
	opm := NewAsyncOp(context.Background())

	openStreamOptions := gocbcore.OpenStreamOptions{}

	if collectionIDs != nil && s.dcpAgent.HasCollectionsSupport() {
		options := &gocbcore.OpenStreamFilterOptions{
			CollectionIDs: []uint32{},
		}

		for id := range collectionIDs {
			options.CollectionIDs = append(options.CollectionIDs, id)
		}

		openStreamOptions.FilterOptions = options
	}

	ch := make(chan error)

	op, err := s.dcpAgent.OpenStream(
		vbID,
		0,
		vbUUID,
		gocbcore.SeqNo(offset.SeqNo),
		0xffffffffffffffff,
		gocbcore.SeqNo(offset.StartSeqNo),
		gocbcore.SeqNo(offset.EndSeqNo),
		observer,
		openStreamOptions,
		func(_ []gocbcore.FailoverEntry, err error) {
			opm.Resolve()

			ch <- err
		},
	)

	err = opm.Wait(op, err)
	if err != nil {
		return err
	}

	err = <-ch

	if err != nil {
		if errors.Is(err, gocbcore.ErrMemdRollback) && s.config.RollbackMitigation.Enabled {
			logger.Log.Printf("need to rollback for vbID: %d", vbID)
			return s.openStreamWithRollback(vbID, vbUUID, gocbcore.SeqNo(offset.SeqNo), observer, openStreamOptions)
		}
	}

	return err
}

func (s *client) observeVbID(vbID uint16, vbUUID gocbcore.VbUUID, replica int) (*gocbcore.ObserveVbResult, error) {
	opm := NewAsyncOp(context.Background())
	ch := make(chan error)

	var response *gocbcore.ObserveVbResult

	op, err := s.agent.ObserveVb(gocbcore.ObserveVbOptions{
		VbID:       vbID,
		VbUUID:     vbUUID,
		ReplicaIdx: replica,
	}, func(result *gocbcore.ObserveVbResult, err error) {
		opm.Resolve()

		response = result

		ch <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return nil, err
	}

	return response, <-ch
}

func (s *client) CloseStream(vbID uint16) error {
	opm := NewAsyncOp(context.Background())

	ch := make(chan error)

	op, err := s.dcpAgent.CloseStream(
		vbID,
		gocbcore.CloseStreamOptions{},
		func(err error) {
			opm.Resolve()

			ch <- err
		},
	)

	err = opm.Wait(op, err)

	if err != nil {
		return err
	}

	return <-ch
}

func (s *client) getCollectionID(scopeName string, collectionName string) (uint32, error) {
	ctx := context.Background()
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	ch := make(chan error)
	var collectionID uint32
	op, err := s.agent.GetCollectionID(
		scopeName,
		collectionName,
		gocbcore.GetCollectionIDOptions{
			Deadline: deadline,
		},
		func(result *gocbcore.GetCollectionIDResult, err error) {
			if err == nil {
				collectionID = result.CollectionID
			}

			opm.Resolve()

			ch <- err
		},
	)
	err = opm.Wait(op, err)

	if err != nil {
		return collectionID, err
	}

	return collectionID, <-ch
}

func (s *client) GetCollectionIDs(scopeName string, collectionNames []string) (map[uint32]string, error) {
	collectionIDs := make(map[uint32]string)

	for _, collectionName := range collectionNames {
		collectionID, err := s.getCollectionID(scopeName, collectionName)
		if err != nil {
			return nil, err
		}

		collectionIDs[collectionID] = collectionName
	}

	return collectionIDs, nil
}

func (s *client) UpsertXattrs(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	path string,
	xattrs interface{},
	expiry uint32,
) error {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	payload, _ := jsoniter.Marshal(xattrs)

	ch := make(chan error)

	op, err := s.metaAgent.MutateIn(gocbcore.MutateInOptions{
		Key: id,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpDictSet,
				Flags: memd.SubdocFlagXattrPath,
				Path:  path,
				Value: payload,
			},
		},
		Expiry:         expiry,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, func(result *gocbcore.MutateInResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return err
	}

	err = <-ch

	return err
}

func (s *client) UpdateDocument(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	value interface{},
	expiry uint32,
) error {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	payload, _ := jsoniter.Marshal(value)

	ch := make(chan error)

	op, err := s.metaAgent.MutateIn(gocbcore.MutateInOptions{
		Key: id,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpSetDoc,
				Value: payload,
			},
		},
		Expiry:         expiry,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, func(result *gocbcore.MutateInResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return err
	}

	err = <-ch

	return err
}

func (s *client) CreatePath(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	path []byte,
	value interface{},
) error {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	payload, _ := jsoniter.Marshal(value)

	ch := make(chan error)

	op, err := s.metaAgent.MutateIn(gocbcore.MutateInOptions{
		Key: id,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpDictSet,
				Value: payload,
				Path:  string(path),
			},
		},
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, func(result *gocbcore.MutateInResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return err
	}

	err = <-ch

	return err
}

func (s *client) CreateDocument(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	value interface{},
	expiry uint32,
) error {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	payload, _ := jsoniter.Marshal(value)

	ch := make(chan error)

	op, err := s.metaAgent.Set(gocbcore.SetOptions{
		Key:            id,
		Value:          payload,
		Flags:          50333696,
		Deadline:       deadline,
		Expiry:         expiry,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, func(result *gocbcore.StoreResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return err
	}

	return <-ch
}

func (s *client) DeleteDocument(ctx context.Context, scopeName string, collectionName string, id []byte) {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	ch := make(chan error)

	op, err := s.metaAgent.Delete(gocbcore.DeleteOptions{
		Key:            id,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, func(result *gocbcore.DeleteResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return
	}

	err = <-ch

	if err != nil {
		return
	}
}

func (s *client) GetXattrs(scopeName string, collectionName string, id []byte, path string) ([]byte, error) {
	opm := NewAsyncOp(context.Background())

	errorCh := make(chan error)
	documentCh := make(chan []byte)

	op, err := s.metaAgent.LookupIn(gocbcore.LookupInOptions{
		Key: id,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpGet,
				Flags: memd.SubdocFlagXattrPath,
				Path:  path,
			},
		},
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, func(result *gocbcore.LookupInResult, err error) {
		opm.Resolve()

		if err == nil {
			documentCh <- result.Ops[0].Value
		} else {
			documentCh <- nil
		}

		errorCh <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return nil, err
	}

	document := <-documentCh
	err = <-errorCh

	return document, err
}

func (s *client) Get(ctx context.Context, scopeName string, collectionName string, id []byte) ([]byte, error) {
	opm := NewAsyncOp(context.Background())

	deadline, _ := ctx.Deadline()

	errorCh := make(chan error)
	documentCh := make(chan []byte)

	op, err := s.metaAgent.Get(gocbcore.GetOptions{
		Key:            id,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, func(result *gocbcore.GetResult, err error) {
		opm.Resolve()

		if err == nil {
			documentCh <- result.Value
		} else {
			documentCh <- nil
		}

		errorCh <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return nil, err
	}

	document := <-documentCh
	err = <-errorCh

	return document, err
}

func NewClient(config *helpers.Config) Client {
	return &client{
		agent:    nil,
		dcpAgent: nil,
		config:   config,
	}
}
