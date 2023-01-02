package godcpclient

import (
	"context"
	"fmt"
	"time"

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
	Close() error
	MetaConnect() error
	MetaClose() error
	DcpConnect() error
	DcpClose() error
	GetBucketUUID() string
	GetVBucketSeqNos() (map[uint16]gocbcore.VbSeqNoEntry, error)
	GetNumVBuckets() int
	GetFailoverLogs(vbIds []uint16) (map[uint16][]gocbcore.FailoverEntry, error)
	OpenStream(
		vbID uint16,
		vbUUID gocbcore.VbUUID,
		collectionIDs map[uint32]string,
		observerState *ObserverState,
		observer Observer,
		callback gocbcore.OpenStreamCallback,
	) error
	CloseStream(vbID uint16, callback gocbcore.CloseStreamCallback) error
	GetCollectionIDs(scopeName string, collectionNames []string) (map[uint32]string, error)
	GetCollectionID(scopeName string, collectionName string) (uint32, error)
}

type client struct {
	agent     *gocbcore.Agent
	metaAgent *gocbcore.Agent
	dcpAgent  *gocbcore.DCPAgent
	config    helpers.Config
}

func (s *client) Ping() (bool, error) {
	opm := NewAsyncOp(context.Background())

	errorCh := make(chan error)
	successCh := make(chan bool)

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

		successCh <- success
		errorCh <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return false, err
	}

	success := <-successCh
	err = <-errorCh

	return success, err
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

	logger.Debug("connected to %s, bucket: %s", s.config.Hosts, s.config.BucketName)

	return nil
}

func (s *client) Close() error {
	logger.Debug("closing connection to %s", s.config.Hosts)
	return s.agent.Close()
}

func (s *client) MetaConnect() error {
	agent, err := s.connect(s.config.MetadataBucket)
	if err != nil {
		return err
	}

	s.metaAgent = agent

	logger.Debug("connected to %s, meta bucket: %s", s.config.Hosts, s.config.MetadataBucket)

	return nil
}

func (s *client) MetaClose() error {
	logger.Debug("closing meta connection to %s", s.config.Hosts)
	return s.metaAgent.Close()
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

func (s *client) DcpClose() error {
	logger.Debug("closing dcp connection to %s", s.config.Hosts)
	return s.dcpAgent.Close()
}

func (s *client) GetBucketUUID() string {
	snapshot, err := s.dcpAgent.ConfigSnapshot()

	if err == nil {
		return snapshot.BucketUUID()
	}

	return ""
}

func (s *client) GetVBucketSeqNos() (map[uint16]gocbcore.VbSeqNoEntry, error) {
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

	seqNos := make(map[uint16]gocbcore.VbSeqNoEntry)

	for i := 1; i <= numNodes; i++ {
		opm := NewAsyncOp(context.Background())

		op, err := s.dcpAgent.GetVbucketSeqnos(
			i,
			memd.VbucketStateActive,
			gocbcore.GetVbucketSeqnoOptions{},
			func(entries []gocbcore.VbSeqNoEntry, err error) {
				for _, en := range entries {
					seqNos[en.VbID] = en
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

func (s *client) GetFailoverLogs(vbIds []uint16) (map[uint16][]gocbcore.FailoverEntry, error) {
	failoverLogs := make(map[uint16][]gocbcore.FailoverEntry)

	for _, vbID := range vbIds {
		opm := NewAsyncOp(context.Background())

		op, err := s.dcpAgent.GetFailoverLog(
			vbID,
			func(entries []gocbcore.FailoverEntry, err error) {
				failoverLogs[vbID] = entries

				opm.Resolve()
			})
		if err != nil {
			return nil, err
		}

		_ = opm.Wait(op, err)
	}

	return failoverLogs, nil
}

func (s *client) OpenStream(
	vbID uint16,
	vbUUID gocbcore.VbUUID,
	collectionIDs map[uint32]string,
	observerState *ObserverState,
	observer Observer,
	callback gocbcore.OpenStreamCallback,
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

	op, err := s.dcpAgent.OpenStream(
		vbID,
		memd.DcpStreamAddFlagActiveOnly,
		vbUUID,
		gocbcore.SeqNo(observerState.SeqNo),
		0xffffffffffffffff,
		gocbcore.SeqNo(observerState.StartSeqNo),
		gocbcore.SeqNo(observerState.EndSeqNo),
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

func (s *client) CloseStream(vbID uint16, callback gocbcore.CloseStreamCallback) error {
	opm := NewAsyncOp(context.Background())

	op, err := s.dcpAgent.CloseStream(
		vbID,
		gocbcore.CloseStreamOptions{},
		func(err error) {
			opm.Resolve()

			callback(err)
		},
	)
	if err != nil {
		return err
	}

	return opm.Wait(op, err)
}

func (s *client) GetCollectionID(scopeName string, collectionName string) (uint32, error) {
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
		collectionID, err := s.GetCollectionID(scopeName, collectionName)
		if err != nil {
			return nil, err
		}

		collectionIDs[collectionID] = collectionName
	}

	return collectionIDs, nil
}

func NewClient(config helpers.Config) Client {
	return &client{
		agent:    nil,
		dcpAgent: nil,
		config:   config,
	}
}
