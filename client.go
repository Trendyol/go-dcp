package main

import (
	"fmt"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"math"
	"time"
)

type Client interface {
	GetAgent() *gocbcore.Agent
	GetDcpAgent() *gocbcore.DCPAgent
	GetGroupName() string
	Connect(hosts []string, username string, password string, userAgent string, bucket string, deadline time.Time, compression bool) error
	Close() error
	DcpConnect(hosts []string, username string, password string, groupName string, userAgent string, bucket string, deadline time.Time, compression bool, bufferSize int) error
	DcpClose() error
	GetVBucketSeqNos() ([]gocbcore.VbSeqNoEntry, error)
	GetNumVBuckets() (int, error)
	GetFailoverLogs(vBucketNumber int) (map[int]gocbcore.FailoverEntry, error)
	OpenStream(vbId uint16, vbUuid gocbcore.VbUUID, observerState ObserverState, observer Observer, callback gocbcore.OpenStreamCallback) error
	CloseStream(vbId uint16, callback gocbcore.CloseStreamCallback) error
}

type client struct {
	agent     *gocbcore.Agent
	dcpAgent  *gocbcore.DCPAgent
	groupName string
}

func (s *client) GetAgent() *gocbcore.Agent {
	return s.agent
}

func (s *client) GetDcpAgent() *gocbcore.DCPAgent {
	return s.dcpAgent
}

func (s *client) GetGroupName() string {
	return s.groupName
}

func (s *client) Connect(hosts []string, username string, password string, userAgent string, bucket string, deadline time.Time, compression bool) error {
	client, err := gocbcore.CreateAgent(
		&gocbcore.AgentConfig{
			UserAgent:  userAgent,
			BucketName: bucket,
			SeedConfig: gocbcore.SeedConfig{
				HTTPAddrs: hosts,
			},
			SecurityConfig: gocbcore.SecurityConfig{
				Auth: gocbcore.PasswordAuthProvider{
					Username: username,
					Password: password,
				},
			},
			CompressionConfig: gocbcore.CompressionConfig{
				Enabled: compression,
			},
		},
	)

	if err != nil {
		return err
	}

	ch := make(chan error)

	_, err = client.WaitUntilReady(
		deadline,
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

	return nil
}

func (s *client) Close() error {
	err := s.agent.Close()
	s.agent = nil
	return err
}

func (s *client) DcpConnect(hosts []string, username string, password string, groupName string, userAgent string, bucket string, deadline time.Time, compression bool, bufferSize int) error {
	client, err := gocbcore.CreateDcpAgent(
		&gocbcore.DCPAgentConfig{
			UserAgent:  userAgent,
			BucketName: bucket,
			SeedConfig: gocbcore.SeedConfig{
				HTTPAddrs: hosts,
			},
			SecurityConfig: gocbcore.SecurityConfig{
				Auth: gocbcore.PasswordAuthProvider{
					Username: username,
					Password: password,
				},
			},
			CompressionConfig: gocbcore.CompressionConfig{
				Enabled: compression,
			},
			DCPConfig: gocbcore.DCPConfig{
				BufferSize: bufferSize,
			},
		},
		groupName,
		memd.DcpOpenFlagProducer,
	)

	if err != nil {
		return err
	}

	ch := make(chan error)

	_, err = client.WaitUntilReady(
		deadline,
		gocbcore.WaitUntilReadyOptions{},
		func(result *gocbcore.WaitUntilReadyResult, err error) {
			ch <- err
		},
	)

	err = <-ch

	if err != nil {
		return err
	}

	s.groupName = groupName
	s.dcpAgent = client

	return nil
}

func (s *client) DcpClose() error {
	err := s.dcpAgent.Close()
	s.dcpAgent = nil
	return err
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

func (s *client) GetFailoverLogs(vBucketNumber int) (map[int]gocbcore.FailoverEntry, error) {
	failoverLogs := make(map[int]gocbcore.FailoverEntry)

	for vbId := 0; vbId < vBucketNumber; vbId++ {
		opm := newAsyncOp(nil)

		op, err := s.dcpAgent.GetFailoverLog(
			uint16(vbId),
			func(entries []gocbcore.FailoverEntry, err error) {
				for _, en := range entries {
					failoverLogs[vbId] = en
				}

				opm.Resolve()
			})

		_ = opm.Wait(op, err)
	}

	return failoverLogs, nil
}

func (s *client) OpenStream(vbId uint16, vbUuid gocbcore.VbUUID, observerState ObserverState, observer Observer, callback gocbcore.OpenStreamCallback) error {
	opm := newAsyncOp(nil)

	op, err := s.dcpAgent.OpenStream(
		vbId,
		memd.DcpStreamAddFlagActiveOnly,
		vbUuid,
		gocbcore.SeqNo(observerState.LastSeqNo),
		math.MaxInt64,
		gocbcore.SeqNo(observerState.LastSnapStart),
		gocbcore.SeqNo(observerState.LastSnapEnd),
		observer,
		gocbcore.OpenStreamOptions{},
		func(entries []gocbcore.FailoverEntry, err error) {
			opm.Resolve()
			callback(entries, err)
		},
	)

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

	return opm.Wait(op, err)
}

func NewClient() Client {
	return &client{
		agent:    nil,
		dcpAgent: nil,
	}
}
