package godcpclient

type Metadata interface {
	Save(state map[uint16]CheckpointDocument, bucketUuid string)
	Load(vbIds []uint16, bucketUuid string) map[uint16]CheckpointDocument
	Clear(vbIds []uint16)
}
