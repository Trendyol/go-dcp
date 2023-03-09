package godcpclient

import (
	"errors"
	"os"

	jsoniter "github.com/json-iterator/go"

	"github.com/Trendyol/go-dcp-client/helpers"
)

type fileMetadata struct {
	fileName string
}

func (s *fileMetadata) Save(state map[uint16]CheckpointDocument, _ string) error {
	file, _ := jsoniter.MarshalIndent(state, "", "  ")
	_ = os.WriteFile(s.fileName, file, 0o644) //nolint:gosec
	return nil
}

func (s *fileMetadata) Load(vbIds []uint16, bucketUUID string) (map[uint16]CheckpointDocument, error) {
	file, err := os.ReadFile(s.fileName)

	state := map[uint16]CheckpointDocument{}

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			for _, vbID := range vbIds {
				state[vbID] = NewEmptyCheckpointDocument(bucketUUID)
			}
		} else {
			return nil, err
		}
	} else {
		_ = jsoniter.Unmarshal(file, &state)
	}

	return state, nil
}

func (s *fileMetadata) Clear(_ []uint16) error {
	_ = os.Remove(s.fileName)
	return nil
}

func _(fileName string, _ helpers.Config) Metadata {
	return &fileMetadata{
		fileName: fileName,
	}
}
