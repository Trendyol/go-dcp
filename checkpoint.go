package godcpclient

import (
	"sync"
	"time"

	"github.com/Trendyol/go-dcp-client/models"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/couchbase/gocbcore/v10"
)

type Checkpoint interface {
	Save()
	Load() map[uint16]models.Offset
	Clear()
	StartSchedule()
	StopSchedule()
}

type checkpointDocumentSnapshot struct {
	StartSeqNo uint64 `json:"startSeqno"`
	EndSeqNo   uint64 `json:"endSeqno"`
}

type checkpointDocumentCheckpoint struct {
	VbUUID   uint64                     `json:"vbuuid"`
	SeqNo    uint64                     `json:"seqno"`
	Snapshot checkpointDocumentSnapshot `json:"snapshot"`
}

type CheckpointDocument struct {
	BucketUUID string                       `json:"bucketUuid"`
	Checkpoint checkpointDocumentCheckpoint `json:"checkpoint"`
}

func NewEmptyCheckpointDocument(bucketUUID string) CheckpointDocument {
	return CheckpointDocument{
		Checkpoint: checkpointDocumentCheckpoint{
			VbUUID: 0,
			SeqNo:  0,
			Snapshot: checkpointDocumentSnapshot{
				StartSeqNo: 0,
				EndSeqNo:   0,
			},
		},
		BucketUUID: bucketUUID,
	}
}

type checkpoint struct {
	stream     Stream
	metadata   Metadata
	schedule   *time.Ticker
	bucketUUID string
	config     helpers.Config
	vbIds      []uint16
	saveLock   sync.Mutex
	loadLock   sync.Mutex
}

func (s *checkpoint) Save() {
	s.saveLock.Lock()
	defer s.saveLock.Unlock()

	dump := map[uint16]CheckpointDocument{}

	s.stream.LockOffsets()

	for vbID, offset := range s.stream.GetOffsets() {
		dump[vbID] = CheckpointDocument{
			Checkpoint: checkpointDocumentCheckpoint{
				VbUUID: uint64(offset.VbUUID),
				SeqNo:  offset.SeqNo,
				Snapshot: checkpointDocumentSnapshot{
					StartSeqNo: offset.StartSeqNo,
					EndSeqNo:   offset.EndSeqNo,
				},
			},
			BucketUUID: s.bucketUUID,
		}
	}

	s.stream.UnlockOffsets()

	s.metadata.Save(dump, s.bucketUUID)

	logger.Debug("saved checkpoint")
}

func (s *checkpoint) Load() map[uint16]models.Offset {
	s.loadLock.Lock()
	defer s.loadLock.Unlock()

	dump := s.metadata.Load(s.vbIds, s.bucketUUID)

	offsets := map[uint16]models.Offset{}

	for vbID, doc := range dump {
		offsets[vbID] = models.Offset{
			SnapshotMarker: models.SnapshotMarker{
				StartSeqNo: doc.Checkpoint.Snapshot.StartSeqNo,
				EndSeqNo:   doc.Checkpoint.Snapshot.EndSeqNo,
			},
			VbUUID: gocbcore.VbUUID(doc.Checkpoint.VbUUID),
			SeqNo:  doc.Checkpoint.SeqNo,
		}
	}

	logger.Debug("loaded checkpoint")

	return offsets
}

func (s *checkpoint) Clear() {
	s.metadata.Clear(s.vbIds)
	logger.Debug("cleared checkpoint")
}

func (s *checkpoint) StartSchedule() {
	if s.config.Checkpoint.Type != helpers.CheckpointTypeAuto {
		return
	}

	go func() {
		s.schedule = time.NewTicker(s.config.Checkpoint.Interval)
		for range s.schedule.C {
			s.Save()
		}
	}()

	logger.Debug("started checkpoint schedule")
}

func (s *checkpoint) StopSchedule() {
	if s.config.Checkpoint.Type != helpers.CheckpointTypeAuto {
		return
	}

	if s.schedule != nil {
		s.schedule.Stop()
	}

	logger.Debug("stopped checkpoint schedule")
}

func NewCheckpoint(
	stream Stream,
	vbIds []uint16,
	bucketUUID string,
	metadata Metadata, config helpers.Config,
) Checkpoint {
	return &checkpoint{
		stream:     stream,
		vbIds:      vbIds,
		bucketUUID: bucketUUID,
		metadata:   metadata,
		config:     config,
	}
}
