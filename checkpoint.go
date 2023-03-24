package godcpclient

import (
	"sync"
	"time"

	gDcp "github.com/Trendyol/go-dcp-client/dcp"

	"github.com/Trendyol/go-dcp-client/models"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/couchbase/gocbcore/v10"
)

type Checkpoint interface {
	Save()
	Load() (map[uint16]*models.Offset, map[uint16]bool, bool)
	Clear()
	StartSchedule()
	StopSchedule()
}

type checkpointDocumentSnapshot struct {
	StartSeqNo uint64 `json:"startSeqno"`
	EndSeqNo   uint64 `json:"endSeqno"`
}

type checkpointDocumentCheckpoint struct {
	VbUUID   uint64                      `json:"vbuuid"`
	SeqNo    uint64                      `json:"seqno"`
	Snapshot *checkpointDocumentSnapshot `json:"snapshot"`
}

type CheckpointDocument struct {
	BucketUUID string                        `json:"bucketUuid"`
	Checkpoint *checkpointDocumentCheckpoint `json:"checkpoint"`
}

func NewEmptyCheckpointDocument(bucketUUID string) *CheckpointDocument {
	return &CheckpointDocument{
		Checkpoint: &checkpointDocumentCheckpoint{
			VbUUID: 0,
			SeqNo:  0,
			Snapshot: &checkpointDocumentSnapshot{
				StartSeqNo: 0,
				EndSeqNo:   0,
			},
		},
		BucketUUID: bucketUUID,
	}
}

type checkpoint struct {
	stream     Stream
	client     gDcp.Client
	metadata   Metadata
	schedule   *time.Ticker
	bucketUUID string
	vbIds      []uint16
	config     *helpers.Config
	saveLock   *sync.Mutex
	loadLock   *sync.Mutex
}

func (s *checkpoint) Save() {
	s.stream.LockOffsets()
	defer s.stream.UnlockOffsets()

	offsets, dirtyOffsets, anyDirtyOffset := s.stream.GetOffsets()

	if !anyDirtyOffset {
		logger.Log.Printf("no need to save checkpoint")
		return
	}

	s.saveLock.Lock()
	defer s.saveLock.Unlock()

	dump := map[uint16]*CheckpointDocument{}

	for vbID, offset := range offsets {
		dump[vbID] = &CheckpointDocument{
			Checkpoint: &checkpointDocumentCheckpoint{
				VbUUID: uint64(offset.VbUUID),
				SeqNo:  offset.SeqNo,
				Snapshot: &checkpointDocumentSnapshot{
					StartSeqNo: offset.StartSeqNo,
					EndSeqNo:   offset.EndSeqNo,
				},
			},
			BucketUUID: s.bucketUUID,
		}
	}

	err := s.metadata.Save(dump, dirtyOffsets, s.bucketUUID)
	if err == nil {
		logger.Log.Printf("saved checkpoint")
		s.stream.UnmarkDirtyOffsets()
	} else {
		logger.ErrorLog.Printf("error while saving checkpoint document: %v", err)
	}
}

func (s *checkpoint) Load() (map[uint16]*models.Offset, map[uint16]bool, bool) {
	s.loadLock.Lock()
	defer s.loadLock.Unlock()

	dump, exist, err := s.metadata.Load(s.vbIds, s.bucketUUID)
	if err == nil {
		logger.Log.Printf("loaded checkpoint")
	} else {
		logger.ErrorLog.Printf("error while loading checkpoint document: %v", err)
		panic(err)
	}

	offsets := map[uint16]*models.Offset{}
	dirtyOffsets := map[uint16]bool{}
	anyDirtyOffset := false

	if !exist && s.config.Checkpoint.AutoReset == helpers.CheckpointAutoResetTypeLatest {
		logger.Log.Printf("no checkpoint found, auto reset checkpoint to latest")

		seqNoMap, err := s.client.GetVBucketSeqNos()
		if err != nil {
			logger.ErrorLog.Printf("error while getting vbucket seqNos: %v", err)
			panic(err)
		}

		for vbID, doc := range dump {
			currentSeqNo := seqNoMap[vbID]

			if currentSeqNo != 0 {
				dirtyOffsets[vbID] = true
				anyDirtyOffset = true
			}

			offsets[vbID] = &models.Offset{
				SnapshotMarker: &models.SnapshotMarker{
					StartSeqNo: currentSeqNo,
					EndSeqNo:   currentSeqNo,
				},
				VbUUID: gocbcore.VbUUID(doc.Checkpoint.VbUUID),
				SeqNo:  currentSeqNo,
			}
		}

		return offsets, dirtyOffsets, anyDirtyOffset
	}

	for vbID, doc := range dump {
		offsets[vbID] = &models.Offset{
			SnapshotMarker: &models.SnapshotMarker{
				StartSeqNo: doc.Checkpoint.Snapshot.StartSeqNo,
				EndSeqNo:   doc.Checkpoint.Snapshot.EndSeqNo,
			},
			VbUUID: gocbcore.VbUUID(doc.Checkpoint.VbUUID),
			SeqNo:  doc.Checkpoint.SeqNo,
		}
	}

	return offsets, dirtyOffsets, anyDirtyOffset
}

func (s *checkpoint) Clear() {
	_ = s.metadata.Clear(s.vbIds)
	logger.Log.Printf("cleared checkpoint")
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

	logger.Log.Printf("started checkpoint schedule")
}

func (s *checkpoint) StopSchedule() {
	if s.config.Checkpoint.Type != helpers.CheckpointTypeAuto {
		return
	}

	if s.schedule != nil {
		s.schedule.Stop()
	}

	logger.Log.Printf("stopped checkpoint schedule")
}

func NewCheckpoint(
	stream Stream,
	vbIds []uint16,
	client gDcp.Client,
	metadata Metadata,
	config *helpers.Config,
) Checkpoint {
	return &checkpoint{
		client:     client,
		stream:     stream,
		vbIds:      vbIds,
		bucketUUID: client.GetBucketUUID(),
		metadata:   metadata,
		config:     config,
		saveLock:   &sync.Mutex{},
		loadLock:   &sync.Mutex{},
	}
}
