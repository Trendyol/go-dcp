package couchbase

import (
	"context"
	"errors"
	"strconv"
	"sync"

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
	config         *helpers.Config
	scopeName      string
	collectionName string
}

func (s *cbMetadata) Save(state map[uint16]*models.CheckpointDocument, dirtyOffsets map[uint16]bool, _ string) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Checkpoint.Timeout)
	defer cancel()

	errCh := make(chan error, 1)

	go func(ctx context.Context) {
		var err error

		for vbID, checkpointDocument := range state {
			if !dirtyOffsets[vbID] {
				continue
			}

			id := getCheckpointID(vbID, s.config.Dcp.Group.Name)
			err = s.client.UpsertXattrs(ctx, s.scopeName, s.collectionName, id, helpers.Name, checkpointDocument, 0)

			var kvErr *gocbcore.KeyValueError
			if err != nil && errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
				err = s.client.CreateDocument(ctx, s.scopeName, s.collectionName, id, []byte{}, 0)

				if err == nil {
					err = s.client.UpsertXattrs(ctx, s.scopeName, s.collectionName, id, helpers.Name, checkpointDocument, 0)
				}
			}

			if err != nil {
				break
			}
		}

		errCh <- err
	}(ctx)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
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

			data, err := s.client.GetXattrs(s.scopeName, s.collectionName, id, helpers.Name)

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

func (s *cbMetadata) Clear(vbIds []uint16) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Checkpoint.Timeout)
	defer cancel()

	for _, vbID := range vbIds {
		id := getCheckpointID(vbID, s.config.Dcp.Group.Name)

		s.client.DeleteDocument(ctx, s.scopeName, s.collectionName, id)
	}

	return nil
}

func NewCBMetadata(client Client, config *helpers.Config) metadata.Metadata {
	if !config.IsCouchbaseMetadata() {
		err := errors.New("unsupported metadata type")
		logger.ErrorLog.Printf("cannot initialize couchbase metadata: %v", err)
		panic(err)
	}

	_, scope, collection, _ := config.GetCouchbaseMetadata()

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
