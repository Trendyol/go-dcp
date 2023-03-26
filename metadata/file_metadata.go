package metadata

import (
	"errors"
	"os"

	"github.com/Trendyol/go-dcp-client/models"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/json-iterator/go"

	"github.com/Trendyol/go-dcp-client/helpers"
)

type fileMetadata struct { //nolint:unused
	fileName string
}

func (s *fileMetadata) Save(state map[uint16]*models.CheckpointDocument, _ map[uint16]bool, _ string) error { //nolint:unused
	file, _ := jsoniter.MarshalIndent(state, "", "  ")
	_ = os.WriteFile(s.fileName, file, 0o644) //nolint:gosec
	return nil
}

func (s *fileMetadata) Load(vbIds []uint16, bucketUUID string) (map[uint16]*models.CheckpointDocument, bool, error) { //nolint:unused
	file, err := os.ReadFile(s.fileName)

	state := map[uint16]*models.CheckpointDocument{}
	exist := true

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			exist = false

			for _, vbID := range vbIds {
				state[vbID] = models.NewEmptyCheckpointDocument(bucketUUID)
			}
		} else {
			return nil, exist, err
		}
	} else {
		_ = jsoniter.Unmarshal(file, &state)
	}

	return state, exist, nil
}

func (s *fileMetadata) Clear(_ []uint16) error { //nolint:unused
	_ = os.Remove(s.fileName)
	return nil
}

func NewFSMetadata(config *helpers.Config) Metadata { //nolint:unused
	if !config.IsFileMetadata() {
		err := errors.New("unsupported metadata type")
		logger.ErrorLog.Printf("cannot initialize file metadata: %s", err)
		panic(err)
	}

	fileName := config.GetFileMetadata()

	return &fileMetadata{
		fileName: fileName,
	}
}
