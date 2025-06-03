package metadata

import (
	"errors"

	"github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/wrapper"

	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp/logger"
)

type noopMetadata struct { //nolint:unused
}

func (s *noopMetadata) Save(state map[uint16]*models.CheckpointDocument, _ map[uint16]bool, _ string) error { //nolint:unused
	return nil
}

func (s *noopMetadata) Load(vbIds []uint16, bucketUUID string) (*wrapper.ConcurrentSwissMap[uint16, *models.CheckpointDocument], bool, error) { //nolint:lll,unused
	state := wrapper.CreateConcurrentSwissMap[uint16, *models.CheckpointDocument](1024)

	for _, vbID := range vbIds {
		state.Store(vbID, models.NewEmptyCheckpointDocument(bucketUUID))
	}

	return state, false, nil
}

func (s *noopMetadata) Clear(_ []uint16) error { //nolint:unused
	return nil
}

func NewNoopMetadata(config *config.Dcp) Metadata { //nolint:unused
	if !config.IsNoopMetadata() {
		err := errors.New("unsupported metadata type")
		logger.Log.Error("error while initialize noop metadata, err: %s", err)
		panic(err)
	}

	return &noopMetadata{}
}
