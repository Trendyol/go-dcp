package couchbase

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Trendyol/go-dcp-client/config"

	"github.com/google/uuid"

	"github.com/json-iterator/go"

	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/models"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

type Client interface {
	Ping() error
	GetAgent() *gocbcore.Agent
	GetMetaAgent() *gocbcore.Agent
	Connect() error
	Close()
	DcpConnect() error
	DcpClose()
	GetVBucketSeqNos() (map[uint16]uint64, error)
	GetNumVBuckets() int
	GetFailoverLogs(vbID uint16) ([]gocbcore.FailoverEntry, error)
	OpenStream(vbID uint16, collectionIDs map[uint32]string, offset *models.Offset, observer Observer) error
	CloseStream(vbID uint16) error
	GetCollectionIDs(scopeName string, collectionNames []string) map[uint32]string
	CreateDocument(ctx context.Context, scopeName string, collectionName string, id []byte, value interface{}, expiry uint32) error
	GetConfigSnapshot() (*gocbcore.ConfigSnapshot, error)
}

type client struct {
	agent     *gocbcore.Agent
	metaAgent *gocbcore.Agent
	dcpAgent  *gocbcore.DCPAgent
	config    *config.Dcp
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

func (s *client) GetMetaAgent() *gocbcore.Agent {
	return s.metaAgent
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

func (s *client) connect(bucketName string, connectionBufferSize uint, connectionTimeout time.Duration) (*gocbcore.Agent, error) {
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
		time.Now().Add(connectionTimeout),
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
	connectionBufferSize := s.config.ConnectionBufferSize
	connectionTimeout := s.config.ConnectionTimeout

	if s.config.IsCouchbaseMetadata() {
		metadataBucketName, _, _, metadataConnectionBufferSize, metadataConnectionTimeout := s.config.GetCouchbaseMetadata()
		if metadataBucketName == s.config.BucketName {
			if metadataConnectionBufferSize > connectionBufferSize {
				connectionBufferSize = metadataConnectionBufferSize
			}

			if metadataConnectionTimeout > connectionTimeout {
				connectionTimeout = metadataConnectionTimeout
			}
		}
	}

	agent, err := s.connect(s.config.BucketName, connectionBufferSize, connectionTimeout)
	if err != nil {
		return err
	}

	s.agent = agent

	if s.config.IsCouchbaseMetadata() {
		metadataBucketName, _, _, metadataConnectionBufferSize, metadataConnectionTimeout := s.config.GetCouchbaseMetadata()
		if metadataBucketName == s.config.BucketName {
			s.metaAgent = agent
		} else {
			metaAgent, err := s.connect(metadataBucketName, metadataConnectionBufferSize, metadataConnectionTimeout)
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
		time.Now().Add(s.config.Dcp.ConnectionTimeout),
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

func (s *client) GetVBucketSeqNos() (map[uint16]uint64, error) {
	snapshot, err := s.GetConfigSnapshot()
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
	snapshot, err := s.GetConfigSnapshot()
	if err != nil {
		logger.ErrorLog.Printf("failed to get config snapshot: %v", err)
		panic(err)
	}

	vBuckets, err := snapshot.NumVbuckets()
	if err != nil {
		logger.ErrorLog.Printf("failed to get number of vbucket: %v", err)
		panic(err)
	}

	return vBuckets
}

func (s *client) GetConfigSnapshot() (*gocbcore.ConfigSnapshot, error) { //nolint:unused
	return s.dcpAgent.ConfigSnapshot()
}

func (s *client) GetFailoverLogs(vbID uint16) ([]gocbcore.FailoverEntry, error) {
	opm := NewAsyncOp(context.Background())
	ch := make(chan error)

	var failoverLogs []gocbcore.FailoverEntry

	op, err := s.dcpAgent.GetFailoverLog(
		vbID,
		func(entries []gocbcore.FailoverEntry, err error) {
			failoverLogs = entries

			opm.Resolve()

			ch <- err
		})

	err = opm.Wait(op, err)
	if err != nil {
		return nil, err
	}

	return failoverLogs, <-ch
}

func (s *client) openStreamWithRollback(vbID uint16,
	failedSeqNo gocbcore.SeqNo,
	rollbackSeqNo gocbcore.SeqNo,
	observer Observer,
	openStreamOptions gocbcore.OpenStreamOptions,
) error {
	logger.Log.Printf(
		"open stream with rollback, vbID: %d, failedSeqNo: %d, rollbackSeqNo: %d",
		vbID, failedSeqNo, rollbackSeqNo,
	)

	opm := NewAsyncOp(context.Background())

	ch := make(chan error)

	op, err := s.dcpAgent.OpenStream(
		vbID,
		0,
		0,
		rollbackSeqNo,
		0xffffffffffffffff,
		rollbackSeqNo,
		rollbackSeqNo,
		observer,
		openStreamOptions,
		func(failoverLogs []gocbcore.FailoverEntry, err error) {
			if len(failoverLogs) > 0 {
				observer.SetVbUUID(vbID, failoverLogs[0].VbUUID)
			}

			observer.AddCatchup(vbID, failedSeqNo)

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
		offset.VbUUID,
		gocbcore.SeqNo(offset.SeqNo),
		0xffffffffffffffff,
		gocbcore.SeqNo(offset.StartSeqNo),
		gocbcore.SeqNo(offset.EndSeqNo),
		observer,
		openStreamOptions,
		func(failoverLogs []gocbcore.FailoverEntry, err error) {
			if len(failoverLogs) > 0 {
				observer.SetVbUUID(vbID, failoverLogs[0].VbUUID)
			}

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
		if rollbackErr, ok := err.(gocbcore.DCPRollbackError); ok {
			logger.Log.Printf("need to rollback for vbID: %d, vbUUID: %d", vbID, offset.VbUUID)
			return s.openStreamWithRollback(vbID, gocbcore.SeqNo(offset.SeqNo), rollbackErr.SeqNo, observer, openStreamOptions)
		}
	}

	return err
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

func (s *client) GetCollectionIDs(scopeName string, collectionNames []string) map[uint32]string {
	collectionIDs := map[uint32]string{}

	if s.config.IsCollectionModeEnabled() {
		for _, collectionName := range collectionNames {
			collectionID, err := s.getCollectionID(scopeName, collectionName)
			if err != nil {
				logger.ErrorLog.Printf("cannot get collection ids: %v", err)
				panic(err)
			}

			collectionIDs[collectionID] = collectionName
		}
	}

	return collectionIDs
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

func NewClient(config *config.Dcp) Client {
	return &client{
		agent:    nil,
		dcpAgent: nil,
		config:   config,
	}
}
