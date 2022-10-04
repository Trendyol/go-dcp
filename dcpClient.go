package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"go-dcp-client/config"
	"math"
	"sync"
	"time"
)

type Agents struct {
	opAgent  *gocbcore.Agent
	dcpAgent *gocbcore.DCPAgent
}

var agnt Agents

// Note that using the infinite DCP stream is limited to a 60s run below, this can be changed
var infinite bool = true //T: Run one infinite DCP stream, or F: run repeated DCP streams (i.e. will not stream events that we've seen before)

func Init(configYmlPath string, onMutation func(gocbcore.DcpMutation), onDeletion func(gocbcore.DcpDeletion)) Agents {

	configYml := config.LoadConfig(configYmlPath)

	hostnameOrIp := "your-cluster-here"
	port := 8091

	var httpHosts []string
	httpHosts = append(httpHosts, fmt.Sprintf("%s:%d", hostnameOrIp, port))

	var authProvider = gocbcore.PasswordAuthProvider{
		Username: configYml.Username,
		Password: configYml.Password,
	}

	agentConfig := gocbcore.AgentConfig{
		UserAgent:  "go-dcp-client",
		BucketName: configYml.Dcp.MetadataBucket,
		SeedConfig: gocbcore.SeedConfig{
			HTTPAddrs: httpHosts,
		},
		SecurityConfig: gocbcore.SecurityConfig{
			Auth: &authProvider,
		},
		HTTPConfig: gocbcore.HTTPConfig{
			ConnectTimeout: 20 * time.Second,
		},
	}

	agent, err := initAgent(agentConfig)
	if err != nil {
		panic(err)
	}
	defer agent.Close()

	flags := memd.DcpOpenFlagProducer

	//All options written out, most using the default values
	dcpConfig := gocbcore.DCPAgentConfig{
		UserAgent:  "go-dcp-client",
		BucketName: configYml.BucketName,
		SeedConfig: gocbcore.SeedConfig{
			MemdAddrs: nil,
			HTTPAddrs: httpHosts,
		},

		SecurityConfig: gocbcore.SecurityConfig{
			UseTLS:            false,
			TLSRootCAProvider: nil,
			NoTLSSeedNode:     false,
			Auth:              &authProvider,
			AuthMechanisms:    nil,
		},

		CompressionConfig: gocbcore.CompressionConfig{
			Enabled:              false,
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
			BufferSize:                   0,
			DisableBufferAcknowledgement: false,
		},
	}

	dcpAgent, err := initDcpAgent(dcpConfig, flags)
	if err != nil {
		panic(err)
	}
	defer dcpAgent.Close()

	agnt = Agents{
		opAgent:  agent,
		dcpAgent: dcpAgent,
	}

	time.Sleep(10 * time.Second)

	//Do a document set operation to have an event to see in DCP
	//A better option is to use a sample bucket to get a bunch of events
	// if err = CBSet(agent); err != nil {
	//	fmt.Println("Got set error")
	//	panic(err)
	//}

	so := &dcpStreamObserver{
		lock:        sync.Mutex{},
		mutations:   make(map[string]Mutation),
		deletions:   make(map[string]Deletion),
		expirations: make(map[string]Deletion),
		dataRange:   make(map[uint16]SeqnoMarker),
		lastSeqno:   make(map[uint16]uint64),
		snapshots:   make(map[uint16]SnapshotMarker),
		endWg:       sync.WaitGroup{},

		OnMutation: onMutation,
		OnDeletion: onDeletion,
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

	return agnt
}

func initAgent(config gocbcore.AgentConfig) (*gocbcore.Agent, error) {
	client, err := gocbcore.CreateAgent(&config)

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

func initDcpAgent(config gocbcore.DCPAgentConfig, openFlags memd.DcpOpenFlag) (*gocbcore.DCPAgent, error) {
	agent, err := gocbcore.CreateDcpAgent(&config, "wills-super-secret-stream", openFlags)
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

	var seqNos []gocbcore.VbSeqNoEntry
	snapshot, err := agnt.dcpAgent.ConfigSnapshot()
	if err != nil {
		fmt.Printf("Config SS failed")
		return
	}

	numNodes, err := snapshot.NumServers()
	if err != nil {
		fmt.Printf("Num servers failed")
		return
	}
	fmt.Printf("Getting VBs. Got %d servers\n", numNodes)

	for i := 1; i < numNodes+1; i++ {
		_, err := agnt.dcpAgent.GetVbucketSeqnos(i, memd.VbucketStateActive, gocbcore.GetVbucketSeqnoOptions{},
			func(entries []gocbcore.VbSeqNoEntry, err error) {
				fmt.Printf("In cb\n")
				if err != nil {
					fmt.Errorf("GetVbucketSeqnos operation failed: %v", err)
					return
				}
				seqNos = append(seqNos, entries...)
			})
		if err != nil {
			fmt.Errorf("got an error doing an op")
		}
	}
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("Running with seqno map: %v\n", seqNos)

	so.endWg.Add(len(seqNos))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var openWg sync.WaitGroup
	openWg.Add(len(seqNos))
	fo, err := getFailOverLogs()
	if err != nil {
		fmt.Printf("Failed to get the failover logs: %v", err)
	}

	//Open stream per vbucket
	for _, entry := range seqNos {
		go func(en gocbcore.VbSeqNoEntry) {
			ch := make(chan error)

			fmt.Printf("(DCP) (%s) (vb %d) Creating DCP stream with start seqno %d, end seqno %d, vbuuid %d, snap start seqno %d, snap end seqno %d\n",
				"default", en.VbID, gocbcore.SeqNo(so.lastSeqno[en.VbID]), en.SeqNo, fo[int(en.VbID)].VbUUID, so.snapshots[en.VbID].lastSnapStart, so.snapshots[en.VbID].lastSnapEnd)

			var err error
			var op gocbcore.PendingOp

			if infinite {
				//Infinite streamer - streams from the beginning to Seq number MaxInt. So it will *never* complete
				//Use this if you want to stream events for a long time and see everything
				op, err = agnt.dcpAgent.OpenStream(en.VbID, memd.DcpStreamAddFlagActiveOnly, fo[int(en.VbID)].VbUUID, gocbcore.SeqNo(so.lastSeqno[en.VbID]), math.MaxInt64,
					0, 0, so, gocbcore.OpenStreamOptions{}, func(entries []gocbcore.FailoverEntry, err error) {
						ch <- err
					},
				)
			} else {
				//Incremental streamer - only receives new events that didn't occur in the last DCP streamer run (the 'memory' is in the DCPStreamObserver)
				op, err = agnt.dcpAgent.OpenStream(en.VbID, memd.DcpStreamAddFlagActiveOnly, fo[int(en.VbID)].VbUUID, gocbcore.SeqNo(so.snapshots[en.VbID].lastSnapEnd), en.SeqNo,
					gocbcore.SeqNo(so.snapshots[en.VbID].lastSnapStart), gocbcore.SeqNo(so.snapshots[en.VbID].lastSnapEnd), so, gocbcore.OpenStreamOptions{}, func(entries []gocbcore.FailoverEntry, err error) {
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
					fmt.Printf("Error received from open stream: %v", err)
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
		fmt.Printf("Failed to open streams")
	case <-wgCh:
		cancel()
		// Let any expirations do their thing
		time.Sleep(5 * time.Second)
	}

	fmt.Printf("All streams open, waiting for streams to complete")

	waitCh := make(chan struct{})
	go func() {
		so.endWg.Wait()
		close(waitCh)
	}()

	select {
	case <-time.After(60 * time.Second):
		fmt.Printf("Timed out waiting for streams to complete")
	case <-waitCh:
	}

	fmt.Printf("All streams complete")

}

// Get the failover entries for all vBuckets (used for getting the VBUUIDs)
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
			op, err := agnt.dcpAgent.GetFailoverLog(vbId, func(entries []gocbcore.FailoverEntry, err error) {
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
					fmt.Printf("Error received from get failover logs: %v", err)
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
