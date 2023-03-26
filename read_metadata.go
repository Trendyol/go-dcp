package godcpclient

type readMetadata struct {
	metadata Metadata
}

func (s *readMetadata) Save(_ map[uint16]*CheckpointDocument, _ map[uint16]bool, _ string) error {
	return nil
}

func (s *readMetadata) Load(vbIds []uint16, bucketUUID string) (map[uint16]*CheckpointDocument, bool, error) {
	return s.metadata.Load(vbIds, bucketUUID)
}

func (s *readMetadata) Clear(_ []uint16) error {
	return nil
}

func NewReadMetadata(metadata Metadata) Metadata {
	return &readMetadata{
		metadata: metadata,
	}
}
