package godcpclient

import (
	"fmt"
	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"log"
	"time"
)

type Client interface {
	Ping() (bool, error)
	GetAgent() *gocbcore.Agent
	Connect() error
	Close() error
	DcpConnect() error
	DcpClose() error
	GetBucketUuid() string
	GetVBucketSeqNos() (map[uint16]gocbcore.VbSeqNoEntry, error)
	GetNumVBuckets() int
	GetFailoverLogs(vbIds []uint16) (map[uint16][]gocbcore.FailoverEntry, error)
	OpenStream(vbId uint16, vbUuid gocbcore.VbUUID, observerState *ObserverState, observer Observer, callback gocbcore.OpenStreamCallback) error
	CloseStream(vbId uint16, callback gocbcore.CloseStreamCallback) error
}

type client struct {
	agent    *gocbcore.Agent
	dcpAgent *gocbcore.DCPAgent
	config   helpers.Config
}

func (s *client) Ping() (bool, error) {
	opm := NewAsyncOp(nil)

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

func (s *client) Connect() error {
	client, err := gocbcore.CreateAgent(
		&gocbcore.AgentConfig{
			BucketName: s.config.MetadataBucket,
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
		return err
	}

	ch := make(chan error)

	_, err = client.WaitUntilReady(
		time.Now().Add(time.Second*10),
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

	s.agent = client
	log.Printf("connected to %s", s.config.Hosts)

	_, err = s.Ping()

	if err != nil {
		return err
	}

	return nil
}

func (s *client) Close() error {
	log.Printf("closing connection to %s", s.config.Hosts)
	return s.agent.Close()
}

func (s *client) DcpConnect() error {
	client, err := gocbcore.CreateDcpAgent(
		&gocbcore.DCPAgentConfig{
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
		},
		helpers.GetDcpStreamName(s.config.Dcp.Group.Name),
		memd.DcpOpenFlagProducer,
	)

	if err != nil {
		return err
	}

	ch := make(chan error)

	_, err = client.WaitUntilReady(
		time.Now().Add(time.Second*10),
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
	log.Printf("connected to %s as dcp", s.config.Hosts)

	return nil
}

func (s *client) DcpClose() error {
	log.Printf("closing dcp connection to %s", s.config.Hosts)
	return s.dcpAgent.Close()
}

func (s *client) GetBucketUuid() string {
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
		opm := NewAsyncOp(nil)

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
		panic(fmt.Errorf("please connect to the dcp first"))
	}

	if snapshot, err := s.dcpAgent.ConfigSnapshot(); err == nil {
		if vBuckets, err := snapshot.NumVbuckets(); err == nil {
			return vBuckets
		} else {
			panic(err)
		}
	} else {
		panic(err)
	}
}

func (s *client) GetFailoverLogs(vbIds []uint16) (map[uint16][]gocbcore.FailoverEntry, error) {
	failoverLogs := make(map[uint16][]gocbcore.FailoverEntry)

	for _, vbId := range vbIds {
		opm := NewAsyncOp(nil)

		op, err := s.dcpAgent.GetFailoverLog(
			vbId,
			func(entries []gocbcore.FailoverEntry, err error) {
				failoverLogs[vbId] = entries

				opm.Resolve()
			})

		if err != nil {
			return nil, err
		}

		_ = opm.Wait(op, err)
	}

	return failoverLogs, nil
}

func (s *client) OpenStream(vbId uint16, vbUuid gocbcore.VbUUID, observerState *ObserverState, observer Observer, callback gocbcore.OpenStreamCallback) error {
	opm := NewAsyncOp(nil)

	op, err := s.dcpAgent.OpenStream(
		vbId,
		memd.DcpStreamAddFlagActiveOnly,
		vbUuid,
		gocbcore.SeqNo(observerState.SeqNo),
		0xffffffffffffffff,
		gocbcore.SeqNo(observerState.StartSeqNo),
		gocbcore.SeqNo(observerState.EndSeqNo),
		observer,
		gocbcore.OpenStreamOptions{},
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

func (s *client) CloseStream(vbId uint16, callback gocbcore.CloseStreamCallback) error {
	opm := NewAsyncOp(nil)

	op, err := s.dcpAgent.CloseStream(
		vbId,
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

func NewClient(config helpers.Config) Client {
	return &client{
		agent:    nil,
		dcpAgent: nil,
		config:   config,
	}
}
