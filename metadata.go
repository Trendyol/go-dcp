package main

type Metadata interface {
	Save(map[uint16]CheckpointDocument, string, string)
	Load([]uint16, string, string) map[uint16]CheckpointDocument
	Clear([]uint16, string)
}
