package main

type Metadata interface {
	Save(map[uint16]CheckpointDocument, string)
	Load([]uint16, string) map[uint16]CheckpointDocument
	Clear([]uint16, string)
}
