package main

import (
	"encoding/json"
	"errors"
	"os"
)

type fileMetadata struct {
	fileName string
}

func (s *fileMetadata) Save(state map[uint16]checkpointDocument, groupName string) {
	file, _ := json.MarshalIndent(state, "", "  ")
	_ = os.WriteFile(s.fileName, file, 0644)
}

func (s *fileMetadata) Load(vbIds []uint16, groupName string) map[uint16]checkpointDocument {
	file, err := os.ReadFile(s.fileName)

	state := map[uint16]checkpointDocument{}

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			for _, vbId := range vbIds {
				state[vbId] = NewCheckpointDocument()
			}
		} else {
			panic(err)
		}
	} else {
		_ = json.Unmarshal(file, &state)
	}

	return state
}

func (s *fileMetadata) Clear(vbIds []uint16, groupName string) {
	_ = os.Remove(s.fileName)
}

func NewFileMetadata(fileName string) Metadata {
	return &fileMetadata{
		fileName: fileName,
	}
}
