package main

type Metadata interface {
	Save(map[uint16]checkpointDocument, string)
	Load([]uint16, string) map[uint16]checkpointDocument
	Clear([]uint16, string)
}
