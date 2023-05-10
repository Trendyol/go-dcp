package couchbase

import (
	"context"
	"errors"
	"golang.org/x/sync/errgroup"
	"strconv"
	"sync"

	"github.com/Trendyol/go-dcp-client/config"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/metadata"
	"github.com/Trendyol/go-dcp-client/models"

	"github.com/json-iterator/go"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

type cbMetadata struct {
	client         Client
	config         *config.Dcp
	scopeName      string
	collectionName string
}

func (s *cbMetadata) Save(state map[uint16]*models.CheckpointDocument, dirtyOffsets map[uint16]bool, _ string) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Checkpoint.Timeout)
	defer cancel()

	eg, _ := errgroup.WithContext(ctx)

	for vbID, checkpointDocument := range state {
		if !dirtyOffsets[vbID] {
			continue
		}
		eg.Go(func() error {
			id := getCheckpointID(vbID, s.config.Dcp.Group.Name)
			err := s.upsertXattrs(ctx, s.scopeName, s.collectionName, id, helpers.Name, checkpointDocument, 0)

			var kvErr *gocbcore.KeyValueError
			if err != nil && errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
				err = s.client.CreateDocument(ctx, s.scopeName, s.collectionName, id, []byte{}, 0)

				if err == nil {
					err = s.upsertXattrs(ctx, s.scopeName, s.collectionName, id, helpers.Name, checkpointDocument, 0)
				}
			}
			return err
		})
	}
	return eg.Wait()
}

func (s *cbMetadata) getXattrs(scopeName string, collectionName string, id []byte, path string) ([]byte, error) {
	opm := NewAsyncOp(context.Background())

	errorCh := make(chan error)
	documentCh := make(chan []byte)

	op, err := s.client.GetMetaAgent().LookupIn(gocbcore.LookupInOptions{
		Key: id,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpGet,
				Flags: memd.SubdocFlagXattrPath,
				Path:  path,
			},
		},
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, func(result *gocbcore.LookupInResult, err error) {
		opm.Resolve()

		if err == nil {
			documentCh <- result.Ops[0].Value
		} else {
			documentCh <- nil
		}

		errorCh <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return nil, err
	}

	document := <-documentCh
	err = <-errorCh

	return document, err
}

func (s *cbMetadata) Load(vbIds []uint16, bucketUUID string) (map[uint16]*models.CheckpointDocument, bool, error) {
	state := map[uint16]*models.CheckpointDocument{}
	stateLock := &sync.Mutex{}

	wg := &sync.WaitGroup{}
	wg.Add(len(vbIds))

	exist := false

	for _, vbID := range vbIds {
		go func(vbID uint16) {
			var err error

			id := getCheckpointID(vbID, s.config.Dcp.Group.Name)

			data, err := s.getXattrs(s.scopeName, s.collectionName, id, helpers.Name)

			var doc *models.CheckpointDocument

			if err == nil {
				err = jsoniter.Unmarshal(data, &doc)

				if err != nil {
					doc = models.NewEmptyCheckpointDocument(bucketUUID)
				} else {
					exist = true
				}
			} else {
				doc = models.NewEmptyCheckpointDocument(bucketUUID)
			}

			var kvErr *gocbcore.KeyValueError
			if err == nil || errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
				stateLock.Lock()
				state[vbID] = doc
				stateLock.Unlock()
			} else {
				logger.ErrorLog.Printf("cannot load checkpoint, vbID: %d, err: %v", vbID, err)
				panic(err)
			}

			wg.Done()
		}(vbID)
	}

	wg.Wait()

	return state, exist, nil
}

func (s *cbMetadata) upsertXattrs(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	path string,
	xattrs interface{},
	expiry uint32,
) error {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	payload, _ := jsoniter.Marshal(xattrs)

	ch := make(chan error)

	op, err := s.client.GetMetaAgent().MutateIn(gocbcore.MutateInOptions{
		Key: id,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpDictSet,
				Flags: memd.SubdocFlagXattrPath,
				Path:  path,
				Value: payload,
			},
		},
		Expiry:         expiry,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
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

func (s *cbMetadata) deleteDocument(ctx context.Context, scopeName string, collectionName string, id []byte) {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	ch := make(chan error)

	op, err := s.client.GetMetaAgent().Delete(gocbcore.DeleteOptions{
		Key:            id,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
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

func (s *cbMetadata) Clear(vbIds []uint16) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Checkpoint.Timeout)
	defer cancel()

	for _, vbID := range vbIds {
		id := getCheckpointID(vbID, s.config.Dcp.Group.Name)

		s.deleteDocument(ctx, s.scopeName, s.collectionName, id)
	}

	return nil
}

func NewCBMetadata(client Client, config *config.Dcp) metadata.Metadata {
	if !config.IsCouchbaseMetadata() {
		err := errors.New("unsupported metadata type")
		logger.ErrorLog.Printf("cannot initialize couchbase metadata: %v", err)
		panic(err)
	}

	_, scope, collection, _, _ := config.GetCouchbaseMetadata()

	return &cbMetadata{
		client:         client,
		config:         config,
		scopeName:      scope,
		collectionName: collection,
	}
}

func getCheckpointID(vbID uint16, groupName string) []byte {
	// _connector:cbgo:groupName:stdout-listener:checkpoint:vbId
	return []byte(helpers.Prefix + groupName + ":checkpoint:" + strconv.Itoa(int(vbID)))
}
