package main

import (
	"encoding/json"
	"github.com/couchbase/gocbcore/v10"
	"os"
)

type Checkpoint interface {
	Save()
	Load() map[int]ObserverState
}

type checkpoint struct {
	observer      Observer
	vBucketNumber int
	failoverLogs  map[int]gocbcore.FailoverEntry
}

func (s *checkpoint) Save() {
	state := s.observer.GetState()

	file, _ := json.MarshalIndent(state, "", "  ")
	_ = os.WriteFile("checkpoint.json", file, 0644)
}

func (s *checkpoint) Load() map[int]ObserverState {
	var observerState = map[int]ObserverState{}
	file, err := os.ReadFile("checkpoint.json")

	if err != nil {
		for i := 0; i < s.vBucketNumber; i++ {
			observerState[i] = ObserverState{}
		}
	} else {
		_ = json.Unmarshal(file, &observerState)
		s.observer.SetState(observerState)
	}

	return observerState
}

func NewCheckpoint(observer Observer, vBucketNumber int, failoverLogs map[int]gocbcore.FailoverEntry) Checkpoint {
	return &checkpoint{
		observer:      observer,
		vBucketNumber: vBucketNumber,
		failoverLogs:  failoverLogs,
	}
}
