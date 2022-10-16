package main

type Metadata interface {
	Save(map[int]checkpointDocument, string)
	Load(int, string) map[int]checkpointDocument
	Clear()
}
