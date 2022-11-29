package godcpclient

import (
	"encoding/json"
	"errors"
	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

type cbMetadata struct {
	agent  *gocbcore.Agent
	config Config
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

	if err != nil {
		return err
	}

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

	if err != nil {
		return
	}

	_ = opm.Wait(op, err)
}

func (s *cbMetadata) getXattrs(id string, path string, bucketUuid string) (CheckpointDocument, error) {
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
				documentCh <- NewEmptyCheckpointDocument(bucketUuid)
			}
		} else {
			documentCh <- NewEmptyCheckpointDocument(bucketUuid)
		}

		errorCh <- err

		opm.Resolve()
	})

	if err != nil {
		return NewEmptyCheckpointDocument(bucketUuid), err
	}

	document := <-documentCh

	err = opm.Wait(op, <-errorCh)

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

	if err != nil {
		return err
	}

	return opm.Wait(op, err)
}

func (s *cbMetadata) Save(state map[uint16]CheckpointDocument, _ string) {
	for vbId, checkpointDocument := range state {
		id := helpers.GetCheckpointId(vbId, s.config.Dcp.Group.Name, s.config.UserAgent)

		err := s.upsertXattrs(id, helpers.Name, checkpointDocument)

		if err != nil {
			_ = s.createEmptyDocument(id)
			_ = s.upsertXattrs(id, helpers.Name, checkpointDocument)
		}
	}
}

func (s *cbMetadata) Load(vbIds []uint16, bucketUuid string) map[uint16]CheckpointDocument {
	state := map[uint16]CheckpointDocument{}

	for _, vbId := range vbIds {
		id := helpers.GetCheckpointId(vbId, s.config.Dcp.Group.Name, s.config.UserAgent)

		data, err := s.getXattrs(id, helpers.Name, bucketUuid)

		var kvErr *gocbcore.KeyValueError
		if err == nil || errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
			state[vbId] = data
		} else {
			panic(err)
		}
	}

	return state
}

func (s *cbMetadata) Clear(vbIds []uint16) {
	for _, vbId := range vbIds {
		id := helpers.GetCheckpointId(vbId, s.config.Dcp.Group.Name, s.config.UserAgent)

		s.deleteDocument(id)
	}
}

func NewCBMetadata(agent *gocbcore.Agent, config Config) Metadata {
	return &cbMetadata{
		agent:  agent,
		config: config,
	}
}
