package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"go-dcp-client/config"
	"log"
	"math"
	"sync"
	"time"
)

type Agents struct {
	opAgent  *gocbcore.Agent
	dcpAgent *gocbcore.DCPAgent
}

var agents Agents

// todo we are keeping it for testing purpose after clarifying state management we can removee
// Note that using the infinite DCP stream is limited to a 60s run below, this can be changed
var infinite = true //T: Run one infinite DCP stream, or F: run repeated DCP streams (i.e. will not stream events that we've seen before)

func InitFromYml(configYmlPath string, onMutation func(Mutation), onDeletion func(Deletion), onExpiration func(Deletion)) {
	couchbaseDCPConfig := config.LoadConfig(configYmlPath)
	Init(couchbaseDCPConfig, onMutation, onDeletion, onExpiration)
}

const port int = 8091

func Init(couchbaseDCPConfig config.CouchbaseDCPConfig, onMutation func(Mutation), onDeletion func(Deletion), onExpiration func(Deletion)) {
	httpHosts := make([]string, 0, len(couchbaseDCPConfig.Hosts))
	for _, host := range couchbaseDCPConfig.Hosts {
		httpHosts = append(httpHosts, fmt.Sprintf("%s:%d", host, port))
	}

	authProvider := gocbcore.PasswordAuthProvider{
		Username: couchbaseDCPConfig.Username,
		Password: couchbaseDCPConfig.Password,
	}

	agent, err := initAgent(&couchbaseDCPConfig, &httpHosts, &authProvider)
	if err != nil {
		panic(err)
	}
	defer agent.Close()

	dcpAgent, err := initDcpAgent(&couchbaseDCPConfig, &httpHosts, &authProvider)
	if err != nil {
		panic(err)
	}
	defer dcpAgent.Close()

	agents = Agents{
		opAgent:  agent,
		dcpAgent: dcpAgent,
	}

	so := &dcpStreamObserver{
		lock: sync.Mutex{},
		// mutations:   make(map[string]Mutation),
		// deletions:   make(map[string]Deletion),
		// expirations: make(map[string]Deletion),
		dataRange: make(map[uint16]seqNoMarker),
		lastSeqNo: make(map[uint16]uint64),
		snapshots: make(map[uint16]snapshotMarker),
		endWg:     sync.WaitGroup{},

		OnMutation:   onMutation,
		OnDeletion:   onDeletion,
		OnExpiration: onExpiration,
	}

	//Run once or run repeatedly
	if infinite {
		setupDcpEventHandler(so)
	} else {
		for {
			setupDcpEventHandler(so)
			time.Sleep(10 * time.Second)
		}
	}
}

func initAgent(couchbaseDCPConfig *config.CouchbaseDCPConfig, httpHosts *[]string, authProvider *gocbcore.PasswordAuthProvider) (*gocbcore.Agent, error) {
	agentConfig := gocbcore.AgentConfig{
		UserAgent:  "go-dcp-client",
		BucketName: couchbaseDCPConfig.Dcp.MetadataBucket,
		SeedConfig: gocbcore.SeedConfig{
			HTTPAddrs: *httpHosts,
		},
		SecurityConfig: gocbcore.SecurityConfig{
			Auth: authProvider,
		},
		HTTPConfig: gocbcore.HTTPConfig{
			ConnectTimeout: 20 * time.Second,
		},
	}

	client, err := gocbcore.CreateAgent(&agentConfig)

	ch := make(chan error)
	_, err = client.WaitUntilReady(
		time.Now().Add(10*time.Second),
		gocbcore.WaitUntilReadyOptions{},
		func(result *gocbcore.WaitUntilReadyResult, err error) {
			ch <- err
		},
	)
	if err != nil {
		return nil, err
	}

	err = <-ch
	if err != nil {
		return nil, err
	}
	return client, nil
}

func initDcpAgent(couchbaseDCPConfig *config.CouchbaseDCPConfig, httpHosts *[]string, authProvider *gocbcore.PasswordAuthProvider) (*gocbcore.DCPAgent, error) {

	//All options written out, most using the default values
	dcpConfig := gocbcore.DCPAgentConfig{
		UserAgent:  "go-dcp-client",
		BucketName: couchbaseDCPConfig.BucketName,
		SeedConfig: gocbcore.SeedConfig{
			MemdAddrs: nil,
			HTTPAddrs: *httpHosts,
		},

		SecurityConfig: gocbcore.SecurityConfig{
			UseTLS:            false,
			TLSRootCAProvider: nil,
			NoTLSSeedNode:     false,
			Auth:              authProvider,
			AuthMechanisms:    nil,
		},

		CompressionConfig: gocbcore.CompressionConfig{
			Enabled:              couchbaseDCPConfig.Dcp.Compression,
			DisableDecompression: false,
			MinSize:              0,
			MinRatio:             0,
		},

		ConfigPollerConfig: gocbcore.ConfigPollerConfig{
			HTTPRedialPeriod: 0,
			HTTPRetryDelay:   0,
			CccpMaxWait:      0,
			CccpPollPeriod:   0,
		},

		IoConfig: gocbcore.IoConfig{
			NetworkType:                 "",
			UseMutationTokens:           false,
			UseDurations:                false,
			UseOutOfOrderResponses:      false,
			DisableXErrorHello:          false,
			DisableJSONHello:            false,
			DisableSyncReplicationHello: false,
			EnablePITRHello:             false,
			UseCollections:              false,
		},

		KVConfig: gocbcore.KVConfig{
			ConnectTimeout:       0,
			ServerWaitBackoff:    0,
			PoolSize:             0,
			MaxQueueSize:         0,
			ConnectionBufferSize: 0,
		},

		HTTPConfig: gocbcore.HTTPConfig{
			MaxIdleConns:          0,
			MaxIdleConnsPerHost:   0,
			ConnectTimeout:        0,
			IdleConnectionTimeout: 0,
		},

		DCPConfig: gocbcore.DCPConfig{
			AgentPriority:                0,
			UseExpiryOpcode:              false,
			UseStreamID:                  false,
			UseOSOBackfill:               false,
			BackfillOrder:                0,
			BufferSize:                   couchbaseDCPConfig.Dcp.FlowControlBuffer,
			DisableBufferAcknowledgement: false,
		},
	}

	var dcpStreamName = couchbaseDCPConfig.Dcp.Group.Name
	agent, err := gocbcore.CreateDcpAgent(&dcpConfig, dcpStreamName, memd.DcpOpenFlagProducer)
	if err != nil {
		return nil, err
	}

	ch := make(chan error)
	_, err = agent.WaitUntilReady(
		time.Now().Add(10*time.Second),
		gocbcore.WaitUntilReadyOptions{},
		func(result *gocbcore.WaitUntilReadyResult, err error) {
			ch <- err
		},
	)
	if err != nil {
		return nil, err
	}

	err = <-ch
	if err != nil {
		return nil, err
	}

	return agent, nil
}

func setupDcpEventHandler(so *dcpStreamObserver) {

	seqNos := getSeqNos()

	log.Printf("Running with seqno map: %v\n", seqNos)

	so.endWg.Add(len(seqNos))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var openWg sync.WaitGroup
	openWg.Add(len(seqNos))
	fo, err := getFailOverLogs()
	if err != nil {
		log.Printf("Failed to get the failover logs: %v", err)
	}

	// todo build membership over vBuckets
	//Open stream per vBucket
	for _, entry := range seqNos {
		go func(en gocbcore.VbSeqNoEntry) {
			ch := make(chan error)

			log.Printf("(DCP) (%s) (vb %d) Creating DCP stream with start seqno %d, end seqno %d, vbuuid %d, snap start seqno %d, snap end seqno %d\n",
				"default", en.VbID, gocbcore.SeqNo(so.lastSeqNo[en.VbID]), en.SeqNo, fo[int(en.VbID)].VbUUID, so.snapshots[en.VbID].lastSnapStart, so.snapshots[en.VbID].lastSnapEnd)

			var err error
			var op gocbcore.PendingOp

			if infinite {
				// todo we are keeping it for testing purpose after clarifying state management we can removee

				//Infinite streamer - streams from the beginning to Seq number MaxInt. So it will *never* complete
				//Use this if you want to stream events for a long time and see everything
				op, err = agents.dcpAgent.OpenStream(
					en.VbID,
					memd.DcpStreamAddFlagActiveOnly,
					fo[int(en.VbID)].VbUUID,
					gocbcore.SeqNo(so.lastSeqNo[en.VbID]),
					math.MaxInt64,
					0,
					0,
					so,
					gocbcore.OpenStreamOptions{},
					func(entries []gocbcore.FailoverEntry, err error) {
						ch <- err
					},
				)
			} else {
				// todo state should come from a durable storage not in-mem

				//Incremental streamer - only receives new events that didn't occur in the last DCP streamer run (the 'memory' is in the DCPStreamObserver)
				op, err = agents.dcpAgent.OpenStream(
					en.VbID,
					memd.DcpStreamAddFlagActiveOnly,
					fo[int(en.VbID)].VbUUID,
					gocbcore.SeqNo(so.snapshots[en.VbID].lastSnapEnd),
					en.SeqNo,
					gocbcore.SeqNo(so.snapshots[en.VbID].lastSnapStart),
					gocbcore.SeqNo(so.snapshots[en.VbID].lastSnapEnd),
					so,
					gocbcore.OpenStreamOptions{},
					func(entries []gocbcore.FailoverEntry, err error) {
						ch <- err
					},
				)
			}

			if err != nil {
				cancel()
				return
			}

			select {
			case err := <-ch:
				if err != nil {
					log.Printf("Error received from open stream: %v", err)
					cancel()
					return
				}
			case <-ctx.Done():
				op.Cancel()
				return
			}

			openWg.Done()
		}(entry)
	}

	wgCh := make(chan struct{}, 1)
	go func() {
		openWg.Wait()
		wgCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		log.Printf("Failed to open streams")
	case <-wgCh:
		cancel()
		// Let any expirations do their thing
		time.Sleep(5 * time.Second)
	}

	log.Printf("All streams open, waiting for streams to complete")

	waitCh := make(chan struct{})
	go func() {
		so.endWg.Wait()
		close(waitCh)
	}()

	select {
	case <-time.After(60 * time.Second):
		log.Printf("Timed out waiting for streams to complete")
	case <-waitCh:
	}

	log.Printf("All streams complete")

}

func getSeqNos() []gocbcore.VbSeqNoEntry {
	var seqNos []gocbcore.VbSeqNoEntry
	snapshot, err := agents.dcpAgent.ConfigSnapshot()
	if err != nil {
		log.Printf("Config SS failed %v", err)
		panic(err)
	}

	numNodes, err := snapshot.NumServers()
	if err != nil {
		log.Printf("Num servers failed %v", err)
		panic(err)
	}
	log.Printf("Getting VBs. Got %d servers\n", numNodes)

	for i := 1; i < numNodes+1; i++ {
		_, err := agents.dcpAgent.GetVbucketSeqnos(i, memd.VbucketStateActive, gocbcore.GetVbucketSeqnoOptions{},
			func(entries []gocbcore.VbSeqNoEntry, err error) {
				log.Printf("In cb\n")
				if err != nil {
					log.Printf("GetVbucketSeqnos operation failed: %v", err)
					return
				}
				seqNos = append(seqNos, entries...)
			})
		if err != nil {
			log.Printf("got an error doing an op %v", err)
		}
	}
	time.Sleep(500 * time.Millisecond)
	return seqNos
}

// Get the failOver entries for all vBuckets (used for getting the VBUUIDs)
func getFailOverLogs() (map[int]gocbcore.FailoverEntry, error) {
	ch := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	failOverEntries := make(map[int]gocbcore.FailoverEntry)

	var openWg sync.WaitGroup
	openWg.Add(1024)
	lock := sync.Mutex{}

	for i := 0; i < 1024; i++ {
		go func(vbId uint16) {
			op, err := agents.dcpAgent.GetFailoverLog(vbId, func(entries []gocbcore.FailoverEntry, err error) {
				for _, en := range entries {
					lock.Lock()
					failOverEntries[int(vbId)] = en
					lock.Unlock()
				}
				ch <- err
			})

			if err != nil {
				cancel()
				return
			}

			select {
			case err := <-ch:
				if err != nil {
					log.Printf("Error received from get failover logs: %v", err)
					cancel()
					return
				}
			case <-ctx.Done():
				op.Cancel()
				return
			}

			openWg.Done()
		}(uint16(i))
	}

	wgCh := make(chan struct{}, 1)
	go func() {
		openWg.Wait()
		wgCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return nil, errors.New("Failed to get failoverlogs")
	case <-wgCh:
		cancel()
	}

	return failOverEntries, nil
}
