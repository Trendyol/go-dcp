package godcpclient

import (
	"encoding/json"
	"errors"
	"os"
)

type fileMetadata struct {
	fileName string
	config   Config
}

func (s *fileMetadata) Save(state map[uint16]CheckpointDocument, _ string) {
	file, _ := json.MarshalIndent(state, "", "  ")
	_ = os.WriteFile(s.fileName, file, 0644)
}

func (s *fileMetadata) Load(vbIds []uint16, bucketUuid string) map[uint16]CheckpointDocument {
	file, err := os.ReadFile(s.fileName)

	state := map[uint16]CheckpointDocument{}

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			for _, vbId := range vbIds {
				state[vbId] = NewEmptyCheckpointDocument(bucketUuid)
			}
		} else {
			panic(err)
		}
	} else {
		_ = json.Unmarshal(file, &state)
	}

	return state
}

func (s *fileMetadata) Clear(_ []uint16) {
	_ = os.Remove(s.fileName)
}

func NewFileMetadata(fileName string, config Config) Metadata {
	return &fileMetadata{
		fileName: fileName,
		config:   config,
	}
}
