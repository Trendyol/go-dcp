package godcpclient

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"log"
	"time"
)

type cbMetadata struct {
	agent  *gocbcore.Agent
	config helpers.Config
}

func (s *cbMetadata) upsertXattrs(ctx context.Context, id string, path string, xattrs interface{}) error {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

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
		Deadline: deadline,
	}, func(result *gocbcore.MutateInResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return err
	}

	err = <-ch

	return err
}

func (s *cbMetadata) deleteDocument(ctx context.Context, id string) {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	ch := make(chan error)

	op, err := s.agent.Delete(gocbcore.DeleteOptions{
		Key:      []byte(id),
		Deadline: deadline,
	}, func(result *gocbcore.DeleteResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return
	}

	err = <-ch

	if err != nil {
		return
	}
}

func (s *cbMetadata) getXattrs(ctx context.Context, id string, path string, bucketUuid string) (CheckpointDocument, error) {
	opm := NewAsyncOp(nil)

	deadline, _ := ctx.Deadline()

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
		Deadline: deadline,
	}, func(result *gocbcore.LookupInResult, err error) {
		opm.Resolve()

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
	})

	err = opm.Wait(op, err)

	if err != nil {
		return NewEmptyCheckpointDocument(bucketUuid), err
	}

	document := <-documentCh
	err = <-errorCh

	return document, err
}

func (s *cbMetadata) createEmptyDocument(ctx context.Context, id string) error {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	ch := make(chan error)

	op, err := s.agent.Set(gocbcore.SetOptions{
		Key:      []byte(id),
		Value:    []byte{},
		Flags:    50333696,
		Deadline: deadline,
	}, func(result *gocbcore.StoreResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return err
	}

	return <-ch
}

func (s *cbMetadata) Save(state map[uint16]CheckpointDocument, _ string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	for vbId, checkpointDocument := range state {
		id := helpers.GetCheckpointId(vbId, s.config.Dcp.Group.Name)
		err := s.upsertXattrs(ctx, id, helpers.Name, checkpointDocument)

		var kvErr *gocbcore.KeyValueError
		if err != nil && errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
			err = s.createEmptyDocument(ctx, id)

			if err == nil {
				err = s.upsertXattrs(ctx, id, helpers.Name, checkpointDocument)
			}
		}

		if err != nil {
			log.Printf("error while saving checkpoint document: %v", err)
			return
		}
	}
}

func (s *cbMetadata) Load(vbIds []uint16, bucketUuid string) map[uint16]CheckpointDocument {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	state := map[uint16]CheckpointDocument{}

	for _, vbId := range vbIds {
		id := helpers.GetCheckpointId(vbId, s.config.Dcp.Group.Name)

		data, err := s.getXattrs(ctx, id, helpers.Name, bucketUuid)

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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	for _, vbId := range vbIds {
		id := helpers.GetCheckpointId(vbId, s.config.Dcp.Group.Name)

		s.deleteDocument(ctx, id)
	}
}

func NewCBMetadata(agent *gocbcore.Agent, config helpers.Config) Metadata {
	return &cbMetadata{
		agent:  agent,
		config: config,
	}
}
