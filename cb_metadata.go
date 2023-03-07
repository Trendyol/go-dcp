package godcpclient

import (
	"context"
	"errors"
	"strconv"

	jsoniter "github.com/json-iterator/go"

	gDcp "github.com/Trendyol/go-dcp-client/dcp"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

type cbMetadata struct {
	client gDcp.Client
	config helpers.Config
}

func (s *cbMetadata) Save(state map[uint16]CheckpointDocument, _ string) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Checkpoint.Timeout)
	defer cancel()

	for vbID, checkpointDocument := range state {
		id := getCheckpointID(vbID, s.config.Dcp.Group.Name)
		err := s.client.UpsertXattrs(ctx, id, helpers.Name, checkpointDocument, 0)

		var kvErr *gocbcore.KeyValueError
		if err != nil && errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
			err = s.client.CreateDocument(ctx, id, []byte{}, 0)

			if err == nil {
				err = s.client.UpsertXattrs(ctx, id, helpers.Name, checkpointDocument, 0)
			}
		}

		if err != nil {
			logger.Error(err, "error while saving checkpoint document")
			return
		}
	}
}

func (s *cbMetadata) Load(vbIds []uint16, bucketUUID string) map[uint16]CheckpointDocument {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Checkpoint.Timeout)
	defer cancel()

	state := map[uint16]CheckpointDocument{}

	for _, vbID := range vbIds {
		id := getCheckpointID(vbID, s.config.Dcp.Group.Name)

		data, err := s.client.GetXattrs(ctx, id, helpers.Name)

		var doc CheckpointDocument

		if err == nil {
			err = jsoniter.Unmarshal(data, &doc)

			if err != nil {
				doc = NewEmptyCheckpointDocument(bucketUUID)
			}
		} else {
			doc = NewEmptyCheckpointDocument(bucketUUID)
		}

		var kvErr *gocbcore.KeyValueError
		if err == nil || errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
			state[vbID] = doc
		} else {
			logger.Panic(err, "error while loading checkpoint document")
		}
	}

	return state
}

func (s *cbMetadata) Clear(vbIds []uint16) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Checkpoint.Timeout)
	defer cancel()

	for _, vbID := range vbIds {
		id := getCheckpointID(vbID, s.config.Dcp.Group.Name)

		s.client.DeleteDocument(ctx, id)
	}
}

func NewCBMetadata(client gDcp.Client, config helpers.Config) Metadata {
	return &cbMetadata{
		client: client,
		config: config,
	}
}

func getCheckpointID(vbID uint16, groupName string) []byte {
	// _connector:cbgo:groupName:stdout-listener:checkpoint:vbId
	return []byte(helpers.Prefix + groupName + ":checkpoint:" + strconv.Itoa(int(vbID)))
}
