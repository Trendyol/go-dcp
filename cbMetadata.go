package main

import (
	"encoding/json"
	"errors"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

type cbMetadata struct {
	agent *gocbcore.Agent
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

func (s *cbMetadata) deleteDocument(id string) {
	opm := newAsyncOp(nil)
	op, err := s.agent.Delete(gocbcore.DeleteOptions{
		Key: []byte(id),
	}, func(result *gocbcore.DeleteResult, err error) {
		opm.Resolve()
	})

	_ = opm.Wait(op, err)
}

func (s *cbMetadata) getXattrs(id string, path string) (CheckpointDocument, error) {
	opm := newAsyncOp(nil)

	errorCh := make(chan error)
	documentCh := make(chan CheckpointDocument)

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
			document := CheckpointDocument{}
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
		Flags: 50333696,
	}, func(result *gocbcore.StoreResult, err error) {
		opm.Resolve()
	})

	return opm.Wait(op, err)
}

func (s *cbMetadata) Save(state map[uint16]CheckpointDocument, groupName string) {
	for vbId, checkpointDocument := range state {
		id := GetCheckpointId(vbId, groupName)

		err := s.upsertXattrs(id, Name, checkpointDocument)

		if err != nil {
			_ = s.createEmptyDocument(id)
			_ = s.upsertXattrs(id, Name, checkpointDocument)
		}
	}
}

func (s *cbMetadata) Load(vbIds []uint16, groupName string) map[uint16]CheckpointDocument {
	state := map[uint16]CheckpointDocument{}

	for _, vbId := range vbIds {
		id := GetCheckpointId(vbId, groupName)

		data, err := s.getXattrs(id, Name)

		var kvErr *gocbcore.KeyValueError
		if err == nil || errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
			state[vbId] = data
		} else {
			panic(err)
		}
	}

	return state
}

func (s *cbMetadata) Clear(vbIds []uint16, groupName string) {
	for _, vbId := range vbIds {
		id := GetCheckpointId(vbId, groupName)

		s.deleteDocument(id)
	}
}

func NewCBMetadata(agent *gocbcore.Agent) Metadata {
	return &cbMetadata{
		agent: agent,
	}
}
