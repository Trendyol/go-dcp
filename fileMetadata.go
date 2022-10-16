package main

import (
	"encoding/json"
	"errors"
	"os"
)

type fileMetadata struct {
}

func (s *fileMetadata) Save(state map[int]checkpointDocument, groupName string) {
	file, _ := json.MarshalIndent(state, "", "  ")
	_ = os.WriteFile("checkpoint.json", file, 0644)
}

func (s *fileMetadata) Load(vBucketNumber int, groupName string) map[int]checkpointDocument {
	file, err := os.ReadFile("checkpoint.json")

	state := map[int]checkpointDocument{}

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			for i := 0; i < vBucketNumber; i++ {
				state[i] = checkpointDocument{
					Checkpoint: checkpointDocumentCheckpoint{
						VbUuid: 0,
						SeqNo:  0,
						Snapshot: checkpointDocumentSnapshot{
							StartSeqNo: 0,
							EndSeqNo:   0,
						},
					},
					BucketUuid: "",
				}
			}
		} else {
			panic(err)
		}
	} else {
		_ = json.Unmarshal(file, &state)
	}

	return state
}

func (s *fileMetadata) Clear() {
	_ = os.Remove("checkpoint.json")
}
