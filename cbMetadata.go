package main

import (
	"encoding/json"
	"errors"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

type cbMetadata struct {
	agent gocbcore.Agent
}

func (s *cbMetadata) upsertXattrs(id string, path string, xattrs interface{}) error {
	opm := newAsyncOp(nil)

	payload, _ := json.Marshal(xattrs)

	ch := make(chan error)

	op, err := s.agent.MutateIn(gocbcore.MutateInOptions{
		Key: []byte(id),
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpDictSet,
				Flags: memd.SubdocFlagXattrPath,
				Path:  path,
				Value: payload,
			},
		},
	}, func(result *gocbcore.MutateInResult, err error) {
		ch <- err
		opm.Resolve()
	})

	err = <-ch

	return opm.Wait(op, err)
}

func (s *cbMetadata) getXattrs(id string, path string) (checkpointDocument, error) {
	opm := newAsyncOp(nil)

	errorCh := make(chan error)
	documentCh := make(chan checkpointDocument)

	op, err := s.agent.LookupIn(gocbcore.LookupInOptions{
		Key: []byte(id),
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpGet,
				Flags: memd.SubdocFlagXattrPath,
				Path:  path,
			},
		},
	}, func(result *gocbcore.LookupInResult, err error) {
		if err == nil {
			document := checkpointDocument{}
			err = json.Unmarshal(result.Ops[0].Value, &document)

			if err == nil {
				documentCh <- document
			} else {
				documentCh <- NewCheckpointDocument()
			}
		} else {
			documentCh <- NewCheckpointDocument()
		}

		errorCh <- err

		opm.Resolve()
	})

	document := <-documentCh

	err = <-errorCh
	err = opm.Wait(op, err)

	return document, err
}

func (s *cbMetadata) createEmptyDocument(id string) error {
	opm := newAsyncOp(nil)
	op, err := s.agent.Set(gocbcore.SetOptions{
		Key:   []byte(id),
		Value: []byte{},
		Flags: 50333696, // todo what does it mean?
	}, func(result *gocbcore.StoreResult, err error) {
		opm.Resolve()
	})

	return opm.Wait(op, err)
}

func (s *cbMetadata) Save(state map[int]checkpointDocument, groupName string) {
	for vbId, checkpointDocument := range state {
		id := GetCheckpointId(vbId, groupName)

		err := s.upsertXattrs(id, Name, checkpointDocument)

		if err != nil {
			_ = s.createEmptyDocument(id)
			_ = s.upsertXattrs(id, Name, checkpointDocument)
		}
	}
}

func (s *cbMetadata) Load(vBucketNumber int, groupName string) map[int]checkpointDocument {
	state := map[int]checkpointDocument{}

	for i := 0; i < vBucketNumber; i++ {
		id := GetCheckpointId(i, groupName)

		data, err := s.getXattrs(id, Name)

		var kvErr *gocbcore.KeyValueError
		if err == nil || errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
			state[i] = data
		} else {
			panic(err)
		}
	}

	return state
}

func (s *cbMetadata) Clear() {
}
