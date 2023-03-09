package dcp

import (
	"context"
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/Trendyol/go-dcp-client/models"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

type Client interface {
	Ping() (bool, error)
	GetAgent() *gocbcore.Agent
	GetMetaAgent() *gocbcore.Agent
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
		offset models.Offset,
		observer Observer,
		callback gocbcore.OpenStreamCallback,
	) error
	CloseStream(vbID uint16) error
	GetCollectionIDs(scopeName string, collectionNames []string) (map[uint32]string, error)
	GetCollectionID(scopeName string, collectionName string) (uint32, error)
	UpsertXattrs(ctx context.Context, id []byte, path string, xattrs interface{}, expiry uint32) error
	CreateDocument(ctx context.Context, id []byte, value interface{}, expiry uint32) error
	ExecuteQuery(ctx context.Context, query []byte) ([][]byte, error)
	UpdateDocument(ctx context.Context, id []byte, value interface{}, expiry uint32) error
	DeleteDocument(ctx context.Context, id []byte)
	GetXattrs(ctx context.Context, id []byte, path string) ([]byte, error)
}

type client struct {
	agent     *gocbcore.Agent
	metaAgent *gocbcore.Agent
	dcpAgent  *gocbcore.DCPAgent
	config    helpers.Config
}

func (s *client) Ping() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.HealthCheck.Timeout)
	defer cancel()

	opm := helpers.NewAsyncOp(ctx)

	errorCh := make(chan error)
	var status bool

	op, err := s.agent.Ping(gocbcore.PingOptions{}, func(result *gocbcore.PingResult, err error) {
		opm.Resolve()

		success := false

		if err == nil {
			if memdServiceResults, ok := result.Services[gocbcore.MemdService]; ok {
				for _, memdServiceResult := range memdServiceResults {
					if memdServiceResult.Error == nil && memdServiceResult.State == gocbcore.PingStateOK {
						success = true
						break
					}
				}
			}
		}

		status = success
		errorCh <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return false, err
	}

	return status, <-errorCh
}

func (s *client) GetAgent() *gocbcore.Agent {
	return s.agent
}

func (s *client) GetMetaAgent() *gocbcore.Agent {
	return s.metaAgent
}

func (s *client) connect(bucketName string) (*gocbcore.Agent, error) {
	client, err := gocbcore.CreateAgent(
		&gocbcore.AgentConfig{
			BucketName: bucketName,
			SeedConfig: gocbcore.SeedConfig{
				HTTPAddrs: s.config.Hosts,
			},
			SecurityConfig: gocbcore.SecurityConfig{
				Auth: gocbcore.PasswordAuthProvider{
					Username: s.config.Username,
					Password: s.config.Password,
				},
			},
			CompressionConfig: gocbcore.CompressionConfig{
				Enabled: true,
			},
		},
	)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	if err = <-ch; err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	return client, nil
}

func (s *client) Connect() error {
	agent, err := s.connect(s.config.BucketName)
	if err != nil {
		return err
	}

	s.agent = agent

	if s.config.MetadataBucket == s.config.BucketName {
		s.metaAgent = agent
	} else {
		metaAgent, err := s.connect(s.config.MetadataBucket)
		if err != nil {
			return err
		}

		s.metaAgent = metaAgent
	}

	logger.Debug("connected to %s, bucket: %s, meta bucket: %s", s.config.Hosts, s.config.BucketName, s.config.MetadataBucket)

	return nil
}

func (s *client) Close() {
	_ = s.metaAgent.Close()

	if s.metaAgent != s.agent {
		_ = s.agent.Close()
	}

	logger.Debug("connections closed %s", s.config.Hosts)
}

func (s *client) DcpConnect() error {
	agentConfig := &gocbcore.DCPAgentConfig{
		BucketName: s.config.BucketName,
		SeedConfig: gocbcore.SeedConfig{
			HTTPAddrs: s.config.Hosts,
		},
		SecurityConfig: gocbcore.SecurityConfig{
			Auth: gocbcore.PasswordAuthProvider{
				Username: s.config.Username,
				Password: s.config.Password,
			},
		},
		CompressionConfig: gocbcore.CompressionConfig{
			Enabled: true,
		},
		DCPConfig: gocbcore.DCPConfig{
			BufferSize:      s.config.Dcp.BufferSizeKb * 1024,
			UseExpiryOpcode: true,
		},
	}

	if s.config.IsCollectionModeEnabled() {
		agentConfig.IoConfig = gocbcore.IoConfig{
			UseCollections: true,
		}
	}

	client, err := gocbcore.CreateDcpAgent(
		agentConfig,
		helpers.GetDcpStreamName(s.config.Dcp.Group.Name),
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
	logger.Debug("connected to %s as dcp, bucket: %s", s.config.Hosts, s.config.BucketName)

	return nil
}

func (s *client) DcpClose() {
	_ = s.dcpAgent.Close()
	logger.Debug("dcp connection closed %s", s.config.Hosts)
}

func (s *client) GetBucketUUID() string {
	snapshot, err := s.dcpAgent.ConfigSnapshot()

	if err == nil {
		return snapshot.BucketUUID()
	}

	return ""
}

func (s *client) GetVBucketSeqNos() (map[uint16]uint64, error) {
	if s.dcpAgent == nil {
		return nil, fmt.Errorf("please connect to the dcp first")
	}

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
		opm := helpers.NewAsyncOp(context.Background())

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
	if s.dcpAgent == nil {
		logger.Panic(fmt.Errorf("dcp agent is nil"), "please connect to the dcp first")
	}

	var err error

	if snapshot, err := s.dcpAgent.ConfigSnapshot(); err == nil {
		if vBuckets, err := snapshot.NumVbuckets(); err == nil {
			return vBuckets
		}
	}

	logger.Panic(err, "failed to get number of vBuckets")
	return 0
}

func (s *client) GetVBucketUUIDMap(vbIds []uint16) (map[uint16]gocbcore.VbUUID, error) {
	uuIDMap := make(map[uint16]gocbcore.VbUUID)

	for _, vbID := range vbIds {
		opm := helpers.NewAsyncOp(context.Background())

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

func (s *client) OpenStream(
	vbID uint16,
	vbUUID gocbcore.VbUUID,
	collectionIDs map[uint32]string,
	offset models.Offset,
	observer Observer,
	callback gocbcore.OpenStreamCallback,
) error {
	opm := helpers.NewAsyncOp(context.Background())

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
		func(entries []gocbcore.FailoverEntry, err error) {
			opm.Resolve()

			callback(entries, err)
		},
	)
	if err != nil {
		return err
	}

	return opm.Wait(op, err)
}

func (s *client) CloseStream(vbID uint16) error {
	opm := helpers.NewAsyncOp(context.Background())

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

func (s *client) GetCollectionID(scopeName string, collectionName string) (uint32, error) {
	ctx := context.Background()
	opm := helpers.NewAsyncOp(ctx)

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
		collectionID, err := s.GetCollectionID(scopeName, collectionName)
		if err != nil {
			return nil, err
		}

		collectionIDs[collectionID] = collectionName
	}

	return collectionIDs, nil
}

func (s *client) UpsertXattrs(ctx context.Context, id []byte, path string, xattrs interface{}, expiry uint32) error {
	opm := helpers.NewAsyncOp(ctx)

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
		Expiry:   expiry,
		Deadline: deadline,
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

func (s *client) UpdateDocument(ctx context.Context, id []byte, value interface{}, expiry uint32) error {
	opm := helpers.NewAsyncOp(ctx)

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
		Expiry:   expiry,
		Deadline: deadline,
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

func (s *client) CreateDocument(ctx context.Context, id []byte, value interface{}, expiry uint32) error {
	opm := helpers.NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	payload, _ := jsoniter.Marshal(value)

	ch := make(chan error)

	op, err := s.metaAgent.Set(gocbcore.SetOptions{
		Key:      id,
		Value:    payload,
		Flags:    50333696,
		Deadline: deadline,
		Expiry:   expiry,
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

func (s *client) ExecuteQuery(ctx context.Context, query []byte) ([][]byte, error) {
	opm := helpers.NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	ch := make(chan error)

	var payload []byte

	payload = append(payload, []byte("{\"statement\":\"")...)
	payload = append(payload, query...)
	payload = append(payload, []byte("\"}")...)

	var result [][]byte

	op, err := s.metaAgent.N1QLQuery(
		gocbcore.N1QLQueryOptions{
			Payload:  payload,
			Deadline: deadline,
		},
		func(reader *gocbcore.N1QLRowReader, err error) {
			if err == nil {
				for {
					row := reader.NextRow()
					if row == nil {
						break
					} else {
						result = append(result, row)
					}
				}
			}

			opm.Resolve()

			ch <- err
		},
	)

	err = opm.Wait(op, err)

	if err != nil {
		return nil, err
	}

	return result, <-ch
}

func (s *client) DeleteDocument(ctx context.Context, id []byte) {
	opm := helpers.NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	ch := make(chan error)

	op, err := s.metaAgent.Delete(gocbcore.DeleteOptions{
		Key:      id,
		Deadline: deadline,
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

func (s *client) GetXattrs(ctx context.Context, id []byte, path string) ([]byte, error) {
	opm := helpers.NewAsyncOp(context.Background())

	deadline, _ := ctx.Deadline()

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
		Deadline: deadline,
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

func NewClient(config helpers.Config) Client {
	return &client{
		agent:    nil,
		dcpAgent: nil,
		config:   config,
	}
}
