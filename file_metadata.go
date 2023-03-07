package godcpclient

import (
	"errors"
	"os"

	jsoniter "github.com/json-iterator/go"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/helpers"
)

type fileMetadata struct { //nolint:unused
	fileName string
	config   helpers.Config
}

func (s *fileMetadata) Save(state map[uint16]CheckpointDocument, _ string) { //nolint:unused
	file, _ := jsoniter.MarshalIndent(state, "", "  ")
	_ = os.WriteFile(s.fileName, file, 0o644) //nolint:gosec
}

func (s *fileMetadata) Load(vbIds []uint16, bucketUUID string) map[uint16]CheckpointDocument { //nolint:unused
	file, err := os.ReadFile(s.fileName)

	state := map[uint16]CheckpointDocument{}

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			for _, vbID := range vbIds {
				state[vbID] = NewEmptyCheckpointDocument(bucketUUID)
			}
		} else {
			logger.Panic(err, "error while loading checkpoint document")
		}
	} else {
		_ = jsoniter.Unmarshal(file, &state)
	}

	return state
}

func (s *fileMetadata) Clear(_ []uint16) { //nolint:unused
	_ = os.Remove(s.fileName)
}

func _(fileName string, config helpers.Config) Metadata {
	return &fileMetadata{
		fileName: fileName,
		config:   config,
	}
}
