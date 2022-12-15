package godcpclient

type Metadata interface {
	Save(state map[uint16]CheckpointDocument, bucketUUID string)
	Load(vbIds []uint16, bucketUUID string) map[uint16]CheckpointDocument
	Clear(vbIds []uint16)
}
