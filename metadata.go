package godcpclient

type Metadata interface {
	Save(state map[uint16]*CheckpointDocument, bucketUUID string) error
	Load(vbIds []uint16, bucketUUID string) (map[uint16]*CheckpointDocument, error)
	Clear(vbIds []uint16) error
}
