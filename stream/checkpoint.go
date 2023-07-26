package stream

import (
	"sync"
	"time"

	"github.com/Trendyol/go-dcp/wrapper"

	"github.com/Trendyol/go-dcp/config"

	"github.com/Trendyol/go-dcp/metadata"

	"github.com/Trendyol/go-dcp/couchbase"

	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp/logger"

	"github.com/couchbase/gocbcore/v10"
)

const (
	CheckpointTypeAuto            = "auto"
	CheckpointAutoResetTypeLatest = "latest"
)

type Checkpoint interface {
	Save()
	Load() (*wrapper.SyncMap[uint16, *models.Offset], *wrapper.SyncMap[uint16, bool], bool)
	Clear()
	StartSchedule()
	StopSchedule()
	GetMetric() *CheckpointMetric
}

type CheckpointMetric struct {
	OffsetWrite        int
	OffsetWriteLatency int64
}

type checkpoint struct {
	stream     Stream
	client     couchbase.Client
	metadata   metadata.Metadata
	schedule   *time.Ticker
	config     *config.Dcp
	saveLock   *sync.Mutex
	loadLock   *sync.Mutex
	metric     *CheckpointMetric
	bucketUUID string
	vbIds      []uint16
}

func (s *checkpoint) Save() {
	offsets, dirtyOffsets, anyDirtyOffset := s.stream.GetOffsets()

	if !anyDirtyOffset {
		logger.Log.Printf("no need to save checkpoint")
		return
	}

	s.saveLock.Lock()
	defer s.saveLock.Unlock()

	checkpointDump := map[uint16]*models.CheckpointDocument{}

	offsets.Range(func(vbID uint16, offset *models.Offset) bool {
		checkpointDump[vbID] = &models.CheckpointDocument{
			Checkpoint: &models.CheckpointDocumentCheckpoint{
				VbUUID: uint64(offset.VbUUID),
				SeqNo:  offset.SeqNo,
				Snapshot: &models.CheckpointDocumentSnapshot{
					StartSeqNo: offset.StartSeqNo,
					EndSeqNo:   offset.EndSeqNo,
				},
			},
			BucketUUID: s.bucketUUID,
		}

		return true
	})

	dirtyOffsetsDump := map[uint16]bool{}
	var dirtyOffsetCount int

	dirtyOffsets.Range(func(vbID uint16, dirt bool) bool {
		if dirt {
			dirtyOffsetCount++
		}

		dirtyOffsetsDump[vbID] = dirt

		return true
	})

	s.metric.OffsetWrite = dirtyOffsetCount

	start := time.Now()

	err := s.metadata.Save(checkpointDump, dirtyOffsetsDump, s.bucketUUID)

	s.metric.OffsetWriteLatency = time.Since(start).Milliseconds()

	if err == nil {
		logger.Log.Printf("saved checkpoint")
		s.stream.UnmarkDirtyOffsets()
	} else {
		logger.ErrorLog.Printf("error while saving checkpoint document: %v", err)
	}
}

func (s *checkpoint) Load() (*wrapper.SyncMap[uint16, *models.Offset], *wrapper.SyncMap[uint16, bool], bool) {
	s.loadLock.Lock()
	defer s.loadLock.Unlock()

	dump, exist, err := s.metadata.Load(s.vbIds, s.bucketUUID)
	if err == nil {
		logger.Log.Printf("loaded checkpoint")
	} else {
		logger.ErrorLog.Printf("error while loading checkpoint document: %v", err)
		panic(err)
	}

	offsets := &wrapper.SyncMap[uint16, *models.Offset]{}
	dirtyOffsets := &wrapper.SyncMap[uint16, bool]{}
	anyDirtyOffset := false

	if !exist && s.config.Checkpoint.AutoReset == CheckpointAutoResetTypeLatest {
		logger.Log.Printf("no checkpoint found, auto reset checkpoint to latest")

		seqNoMap, err := s.client.GetVBucketSeqNos()
		if err != nil {
			logger.ErrorLog.Printf("error while getting vbucket seqNos: %v", err)
			panic(err)
		}

		dump.Range(func(vbID uint16, doc *models.CheckpointDocument) bool {
			currentSeqNo := seqNoMap[vbID]

			if currentSeqNo != 0 {
				dirtyOffsets.Store(vbID, true)
				anyDirtyOffset = true
			}

			offsets.Store(vbID, &models.Offset{
				SnapshotMarker: &models.SnapshotMarker{
					StartSeqNo: currentSeqNo,
					EndSeqNo:   currentSeqNo,
				},
				VbUUID: gocbcore.VbUUID(doc.Checkpoint.VbUUID),
				SeqNo:  currentSeqNo,
			})

			return true
		})

		return offsets, dirtyOffsets, anyDirtyOffset
	}

	dump.Range(func(vbID uint16, doc *models.CheckpointDocument) bool {
		offsets.Store(vbID, &models.Offset{
			SnapshotMarker: &models.SnapshotMarker{
				StartSeqNo: doc.Checkpoint.Snapshot.StartSeqNo,
				EndSeqNo:   doc.Checkpoint.Snapshot.EndSeqNo,
			},
			VbUUID: gocbcore.VbUUID(doc.Checkpoint.VbUUID),
			SeqNo:  doc.Checkpoint.SeqNo,
		})

		return true
	})

	return offsets, dirtyOffsets, anyDirtyOffset
}

func (s *checkpoint) Clear() {
	_ = s.metadata.Clear(s.vbIds)
	logger.Log.Printf("cleared checkpoint")
}

func (s *checkpoint) StartSchedule() {
	if s.config.Checkpoint.Type != CheckpointTypeAuto {
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
	if s.config.Checkpoint.Type != CheckpointTypeAuto {
		return
	}

	if s.schedule != nil {
		s.schedule.Stop()
	}

	logger.Log.Printf("stopped checkpoint schedule")
}

func (s *checkpoint) GetMetric() *CheckpointMetric {
	return s.metric
}

func getBucketUUID(client couchbase.Client) string {
	snapshot, err := client.GetConfigSnapshot()
	if err != nil {
		logger.ErrorLog.Printf("failed to get config snapshot: %v", err)
		panic(err)
	}

	return snapshot.BucketUUID()
}

func NewCheckpoint(
	stream Stream,
	vbIds []uint16,
	client couchbase.Client,
	metadata metadata.Metadata,
	config *config.Dcp,
) Checkpoint {
	return &checkpoint{
		client:     client,
		stream:     stream,
		vbIds:      vbIds,
		bucketUUID: getBucketUUID(client),
		metadata:   metadata,
		config:     config,
		saveLock:   &sync.Mutex{},
		loadLock:   &sync.Mutex{},
		metric:     &CheckpointMetric{},
	}
}
