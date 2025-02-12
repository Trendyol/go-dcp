package metadata

import (
	"errors"
	"os"

	"github.com/bytedance/sonic"

	"github.com/Trendyol/go-dcp/wrapper"

	"github.com/Trendyol/go-dcp/config"

	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp/logger"
)

type fileMetadata struct { //nolint:unused
	fileName string
}

func (s *fileMetadata) Save(state map[uint16]*models.CheckpointDocument, _ map[uint16]bool, _ string) error { //nolint:unused
	file, _ := sonic.MarshalIndent(state, "", "  ")
	_ = os.WriteFile(s.fileName, file, 0o644) //nolint:gosec
	return nil
}

func (s *fileMetadata) Load(vbIds []uint16, bucketUUID string) (*wrapper.ConcurrentSwissMap[uint16, *models.CheckpointDocument], bool, error) { //nolint:lll,unused
	file, err := os.ReadFile(s.fileName)

	state := wrapper.CreateConcurrentSwissMap[uint16, *models.CheckpointDocument](1024)
	exist := true

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			exist = false

			for _, vbID := range vbIds {
				state.Store(vbID, models.NewEmptyCheckpointDocument(bucketUUID))
			}
		} else {
			return nil, exist, err
		}
	} else {
		_ = state.UnmarshalJSON(file)
	}

	return state, exist, nil
}

func (s *fileMetadata) Clear(_ []uint16) error { //nolint:unused
	_ = os.Remove(s.fileName)
	return nil
}

func NewFSMetadata(config *config.Dcp) Metadata { //nolint:unused
	if !config.IsFileMetadata() {
		err := errors.New("unsupported metadata type")
		logger.Log.Error("error while initialize file metadata, err: %s", err)
		panic(err)
	}

	return &fileMetadata{
		fileName: config.GetFileMetadata(),
	}
}
