package stream

import (
	"errors"
	"github.com/Trendyol/go-dcp/stream/offset"
	"github.com/couchbase/gocbcore/v10"
	"sync"
	"time"

	"github.com/Trendyol/go-dcp/wrapper"

	"github.com/Trendyol/go-dcp/config"

	"github.com/Trendyol/go-dcp/metadata"

	"github.com/Trendyol/go-dcp/couchbase"

	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp/logger"
)

const (
	CheckpointTypeAuto            = "auto"
	CheckpointAutoResetTypeLatest = "latest"
)

type Checkpoint interface {
	Save()
	Load() (*wrapper.ConcurrentSwissMap[uint16, *models.Offset], *wrapper.ConcurrentSwissMap[uint16, bool], bool)
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
	stream                Stream
	client                couchbase.Client
	metadata              metadata.Metadata
	config                *config.Dcp
	saveLock              *sync.Mutex
	loadLock              *sync.Mutex
	metric                *CheckpointMetric
	bucketUUID            string
	vbIds                 []uint16
	running               bool
	offsetLatestSeqNoInit *offset.OffsetLatestSeqNoInit
}

func (s *checkpoint) Save() {
	offsets, dirtyOffsets, anyDirtyOffset := s.stream.GetOffsets()

	if !anyDirtyOffset {
		logger.Log.Trace("no need to save checkpoint")
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
		logger.Log.Trace("saved checkpoint")
		s.stream.UnmarkDirtyOffsets()
	} else {
		logger.Log.Error("error while saving checkpoint document: %v", err)
	}
}

//nolint:funlen
func (s *checkpoint) Load() (*wrapper.ConcurrentSwissMap[uint16, *models.Offset], *wrapper.ConcurrentSwissMap[uint16, bool], bool) {
	s.loadLock.Lock()
	defer s.loadLock.Unlock()

	dump, exist, err := s.metadata.Load(s.vbIds, s.bucketUUID)
	if err == nil {
		logger.Log.Debug("loaded checkpoint")
	} else {
		logger.Log.Error("error while loading checkpoint document, err: %v", err)
		panic(err)
	}

	seqNoMap, err := s.client.GetVBucketSeqNos(false)
	if err != nil {
		logger.Log.Error("error while getting vBucket seqNos, err: %v", err)
		panic(err)
	}

	offsets := wrapper.CreateConcurrentSwissMap[uint16, *models.Offset](1024)
	dirtyOffsets := wrapper.CreateConcurrentSwissMap[uint16, bool](1024)
	anyDirtyOffset := false

	if !exist && s.config.Checkpoint.AutoReset == CheckpointAutoResetTypeLatest {
		logger.Log.Debug("no checkpoint found, auto reset checkpoint to latest")

		dump.Range(func(vbID uint16, doc *models.CheckpointDocument) bool {
			currentSeqNo, _ := seqNoMap.Load(vbID)

			if currentSeqNo != 0 {
				dirtyOffsets.Store(vbID, true)
				anyDirtyOffset = true
			}

			failOverLogs, err := s.client.GetFailOverLogs(vbID)
			if err != nil {
				logger.Log.Error("error while get failOver logs when initialize latest, err: %v", err)
				panic(err)
			}

			latestOffsetSeqNo := s.offsetLatestSeqNoInit.InitializeLatestSeqNo(currentSeqNo)

			offsets.Store(vbID, &models.Offset{
				SnapshotMarker: &models.SnapshotMarker{
					StartSeqNo: currentSeqNo,
					EndSeqNo:   currentSeqNo,
				},
				VbUUID:      failOverLogs[0].VbUUID,
				SeqNo:       currentSeqNo,
				LatestSeqNo: latestOffsetSeqNo,
			})

			return true
		})

		return offsets, dirtyOffsets, anyDirtyOffset
	}

	dump.Range(func(vbID uint16, doc *models.CheckpointDocument) bool {
		latestSeqNo, _ := seqNoMap.Load(vbID)
		if doc.Checkpoint.SeqNo > latestSeqNo {
			err := errors.New("checkpoint seqNo bigger then vBucket latest seqNo")
			logger.Log.Error(
				"error while loading checkpoint, vbID: %v, checkpoint seqNo: %v, latest seqNo: %v, err: %v",
				vbID, doc.Checkpoint.SeqNo, latestSeqNo, err,
			)
			panic(err)
		}

		latestOffsetSeqNo := s.offsetLatestSeqNoInit.InitializeLatestSeqNo(latestSeqNo)

		offsets.Store(vbID, &models.Offset{
			SnapshotMarker: &models.SnapshotMarker{
				StartSeqNo: doc.Checkpoint.Snapshot.StartSeqNo,
				EndSeqNo:   doc.Checkpoint.Snapshot.EndSeqNo,
			},
			VbUUID:      gocbcore.VbUUID(doc.Checkpoint.VbUUID),
			SeqNo:       doc.Checkpoint.SeqNo,
			LatestSeqNo: latestOffsetSeqNo,
		})

		return true
	})

	return offsets, dirtyOffsets, anyDirtyOffset
}

func (s *checkpoint) Clear() {
	_ = s.metadata.Clear(s.vbIds)
	logger.Log.Debug("cleared checkpoint")
}

func (s *checkpoint) StartSchedule() {
	if s.config.Checkpoint.Type != CheckpointTypeAuto {
		return
	}

	go func() {
		s.running = true
		for s.running {
			time.Sleep(s.config.Checkpoint.Interval)
			s.Save()
		}
	}()

	logger.Log.Debug("started checkpoint schedule")
}

func (s *checkpoint) StopSchedule() {
	if s.config.Checkpoint.Type != CheckpointTypeAuto {
		return
	}

	s.running = false

	logger.Log.Debug("stopped checkpoint schedule")
}

func (s *checkpoint) GetMetric() *CheckpointMetric {
	return s.metric
}

func getBucketUUID(client couchbase.Client) string {
	snapshot, err := client.GetDcpAgentConfigSnapshot()
	if err != nil {
		logger.Log.Error("error while get config snapshot, err: %v", err)
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
	offsetLatestSeqNoInit *offset.OffsetLatestSeqNoInit,
) Checkpoint {
	return &checkpoint{
		client:                client,
		stream:                stream,
		vbIds:                 vbIds,
		bucketUUID:            getBucketUUID(client),
		metadata:              metadata,
		config:                config,
		saveLock:              &sync.Mutex{},
		loadLock:              &sync.Mutex{},
		metric:                &CheckpointMetric{},
		offsetLatestSeqNoInit: offsetLatestSeqNoInit,
	}
}
