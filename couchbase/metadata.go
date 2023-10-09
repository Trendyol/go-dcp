package couchbase

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"sync"

	"github.com/Trendyol/go-dcp/wrapper"

	"golang.org/x/sync/errgroup"

	"github.com/Trendyol/go-dcp/config"

	"github.com/Trendyol/go-dcp/helpers"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/metadata"
	"github.com/Trendyol/go-dcp/models"

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

	for vbID := range state {
		if dirtyOffsets[vbID] {
			eg.Go(s.saveVBucketCheckpoint(ctx, vbID, state[vbID]))
		}
	}
	return eg.Wait()
}

func (s *cbMetadata) saveVBucketCheckpoint(ctx context.Context, vbID uint16, checkpointDocument *models.CheckpointDocument) func() error {
	return func() error {
		id := getCheckpointID(vbID, s.config.Dcp.Group.Name)
		payload, _ := jsoniter.Marshal(checkpointDocument)
		err := UpsertXattrs(ctx, s.client.GetMetaAgent(), s.scopeName, s.collectionName, id, helpers.Name, payload, 0)

		var kvErr *gocbcore.KeyValueError
		if err != nil && errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
			err = CreateDocument(ctx, s.client.GetMetaAgent(), s.scopeName, s.collectionName, id, []byte{}, helpers.JSONFlags, 0)

			if err == nil {
				err = UpsertXattrs(ctx, s.client.GetMetaAgent(), s.scopeName, s.collectionName, id, helpers.Name, payload, 0)
			}
		}
		return err
	}
}

func (s *cbMetadata) Load(
	vbIds []uint16,
	bucketUUID string,
) (*wrapper.ConcurrentSwissMap[uint16, *models.CheckpointDocument], bool, error) {
	state := wrapper.CreateConcurrentSwissMap[uint16, *models.CheckpointDocument](1024)

	wg := &sync.WaitGroup{}
	wg.Add(len(vbIds))

	exist := false

	for _, vbID := range vbIds {
		go func(vbID uint16) {
			var err error

			id := getCheckpointID(vbID, s.config.Dcp.Group.Name)

			data, err := GetXattrs(context.Background(), s.client.GetMetaAgent(), s.scopeName, s.collectionName, id, helpers.Name)

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
				state.Store(vbID, doc)
			} else {
				logger.Log.Error("cannot load checkpoint, vbID: %d, err: %v", vbID, err)
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

		err := DeleteDocument(ctx, s.client.GetMetaAgent(), s.scopeName, s.collectionName, id)
		if err != nil {
			return err
		}
	}

	return nil
}

func NewCBMetadata(client Client, config *config.Dcp) metadata.Metadata {
	if !config.IsCouchbaseMetadata() {
		err := errors.New("unsupported metadata type")
		logger.Log.Error("cannot initialize couchbase metadata: %v", err)
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
	return []byte(helpers.Prefix + url.QueryEscape(groupName) + ":checkpoint:" + strconv.Itoa(int(vbID)))
}
