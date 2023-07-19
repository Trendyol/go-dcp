package metadata

import (
	"github.com/Trendyol/go-dcp/models"
	"github.com/Trendyol/go-dcp/wrapper"
)

type Metadata interface {
	Save(state map[uint16]*models.CheckpointDocument, dirtyOffsets map[uint16]bool, bucketUUID string) error
	Load(vbIds []uint16, bucketUUID string) (*wrapper.SyncMap[uint16, *models.CheckpointDocument], bool, error)
	Clear(vbIds []uint16) error
}
