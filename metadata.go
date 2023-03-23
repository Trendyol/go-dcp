package godcpclient

type Metadata interface {
	Save(state map[uint16]*CheckpointDocument, dirtyOffsets map[uint16]bool, bucketUUID string) error
	Load(vbIds []uint16, bucketUUID string) (map[uint16]*CheckpointDocument, bool, error)
	Clear(vbIds []uint16) error
}
