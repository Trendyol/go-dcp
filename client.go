package godcpclient

import (
	"fmt"
	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/kubernetes"
	"github.com/Trendyol/go-dcp-client/membership"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"log"
	"time"
)

type Client interface {
	GetAgent() *gocbcore.Agent
	GetMembership() membership.Membership
	Connect() error
	Close() error
	DcpConnect() error
	DcpClose() error
	GetBucketUuid() string
	GetVBucketSeqNos() ([]gocbcore.VbSeqNoEntry, error)
	GetNumVBuckets() (int, error)
	GetFailoverLogs(vbIds []uint16) (map[uint16]gocbcore.FailoverEntry, error)
	OpenStream(vbId uint16, vbUuid gocbcore.VbUUID, observerState *ObserverState, observer Observer, callback gocbcore.OpenStreamCallback) error
	CloseStream(vbId uint16, callback gocbcore.CloseStreamCallback) error
}

type client struct {
	agent      *gocbcore.Agent
	dcpAgent   *gocbcore.DCPAgent
	config     Config
	membership membership.Membership
}

func (s *client) GetAgent() *gocbcore.Agent {
	return s.agent
}

func (s *client) GetMembership() membership.Membership {
	return s.membership
}

func (s *client) Connect() error {
	client, err := gocbcore.CreateAgent(
		&gocbcore.AgentConfig{
			UserAgent:  s.config.UserAgent,
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
				Enabled: s.config.Compression,
			},
			ConfigPollerConfig: gocbcore.ConfigPollerConfig{
				CccpPollPeriod: s.config.Dcp.PersistencePollingInterval,
			},
		},
	)

	if err != nil {
		return err
	}

	ch := make(chan error)

	_, err = client.WaitUntilReady(
		time.Now().Add(s.config.ConnectTimeout),
		gocbcore.WaitUntilReadyOptions{},
		func(result *gocbcore.WaitUntilReadyResult, err error) {
			ch <- err
		},
	)

	err = <-ch

	if err != nil {
		return err
	}

	s.agent = client
	log.Printf("connected to %s", s.config.Hosts)

	return nil
}

func (s *client) Close() error {
	log.Printf("closing connection to %s", s.config.Hosts)
	return s.agent.Close()
}

func (s *client) DcpConnect() error {
	client, err := gocbcore.CreateDcpAgent(
		&gocbcore.DCPAgentConfig{
			UserAgent:  s.config.UserAgent,
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
				Enabled: s.config.Compression,
			},
			ConfigPollerConfig: gocbcore.ConfigPollerConfig{
				CccpPollPeriod: s.config.Dcp.PersistencePollingInterval,
			},
			DCPConfig: gocbcore.DCPConfig{
				BufferSize: s.config.Dcp.FlowControlBuffer,
			},
		},
		s.config.Dcp.Group.Name,
		memd.DcpOpenFlagProducer,
	)

	if err != nil {
		return err
	}

	ch := make(chan error)

	_, err = client.WaitUntilReady(
		time.Now().Add(s.config.Dcp.ConnectTimeout),
		gocbcore.WaitUntilReadyOptions{},
		func(result *gocbcore.WaitUntilReadyResult, err error) {
			ch <- err
		},
	)

	err = <-ch

	if err != nil {
		return err
	}

	if err != nil {
		return err
	}

	s.dcpAgent = client
	log.Printf("connected to %s as dcp", s.config.Hosts)

	vBucketNumber, err := s.GetNumVBuckets()

	if s.config.Dcp.Group.Membership.Type == helpers.StaticMembershipType {
		s.membership = membership.NewStaticMembership(
			s.config.Dcp.Group.Membership.MemberNumber,
			s.config.Dcp.Group.Membership.TotalMembers,
			vBucketNumber,
		)
	} else if s.config.Dcp.Group.Membership.Type == helpers.KubernetesStatefulSetMembershipType {
		s.membership = kubernetes.NewKubernetesStatefulSetMembership(
			s.config.Dcp.Group.Membership.TotalMembers,
			vBucketNumber,
		)
	} else {
		return fmt.Errorf("unknown membership type %s", s.config.Dcp.Group.Membership.Type)
	}

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

func (s *client) GetVBucketSeqNos() ([]gocbcore.VbSeqNoEntry, error) {
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

	var seqNos []gocbcore.VbSeqNoEntry

	for i := 1; i <= numNodes; i++ {
		opm := newAsyncOp(nil)

		op, err := s.dcpAgent.GetVbucketSeqnos(
			i,
			memd.VbucketStateActive,
			gocbcore.GetVbucketSeqnoOptions{},
			func(entries []gocbcore.VbSeqNoEntry, err error) {
				if err == nil {
					seqNos = append(seqNos, entries...)
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

func (s *client) GetNumVBuckets() (int, error) {
	if s.dcpAgent == nil {
		return 0, fmt.Errorf("please connect to the dcp first")
	}

	snapshot, err := s.dcpAgent.ConfigSnapshot()

	if err == nil {
		return snapshot.NumVbuckets()
	}

	return 0, err
}

func (s *client) GetFailoverLogs(vbIds []uint16) (map[uint16]gocbcore.FailoverEntry, error) {
	failoverLogs := make(map[uint16]gocbcore.FailoverEntry)

	for _, vbId := range vbIds {
		opm := newAsyncOp(nil)

		op, err := s.dcpAgent.GetFailoverLog(
			vbId,
			func(entries []gocbcore.FailoverEntry, err error) {
				for _, en := range entries {
					failoverLogs[vbId] = en
				}

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
	opm := newAsyncOp(nil)

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
	opm := newAsyncOp(nil)

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

func NewClient(config Config) Client {
	return &client{
		agent:    nil,
		dcpAgent: nil,
		config:   config,
	}
}
