package metric

import (
	"strconv"

	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp/couchbase"
	"github.com/Trendyol/go-dcp/helpers"
	"github.com/Trendyol/go-dcp/stream"

	"github.com/prometheus/client_golang/prometheus"
)

type metricCollector struct {
	stream           stream.Stream
	client           couchbase.Client
	vBucketDiscovery stream.VBucketDiscovery

	mutation   *prometheus.Desc
	deletion   *prometheus.Desc
	expiration *prometheus.Desc

	agentQueueCurrent *prometheus.Desc
	agentQueueMax     *prometheus.Desc

	currentSeqNo *prometheus.Desc
	startSeqNo   *prometheus.Desc
	endSeqNo     *prometheus.Desc
	persistSeqNo *prometheus.Desc

	processLatency *prometheus.Desc
	dcpLatency     *prometheus.Desc
	rebalance      *prometheus.Desc

	lag      *prometheus.Desc
	totalLag *prometheus.Desc

	activeStream      *prometheus.Desc
	totalMembers      *prometheus.Desc
	memberNumber      *prometheus.Desc
	membershipType    *prometheus.Desc
	vBucketCount      *prometheus.Desc
	vBucketRangeStart *prometheus.Desc
	vBucketRangeEnd   *prometheus.Desc

	offsetWrite        *prometheus.Desc
	offsetWriteLatency *prometheus.Desc
}

func (s *metricCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(s, ch)
}

//nolint:funlen
func (s *metricCollector) Collect(ch chan<- prometheus.Metric) {
	observers := s.stream.GetObservers()
	if observers == nil {
		return
	}

	seqNoMap, err := s.client.GetVBucketSeqNos()

	observers.Range(func(vbID uint16, observer couchbase.Observer) bool {
		ch <- prometheus.MustNewConstMetric(
			s.persistSeqNo,
			prometheus.CounterValue,
			float64(observer.GetPersistSeqNo()),
			strconv.Itoa(int(vbID)),
		)

		metrics := observer.GetMetrics()

		ch <- prometheus.MustNewConstMetric(
			s.mutation,
			prometheus.CounterValue,
			metrics.TotalMutations,
			strconv.Itoa(int(vbID)),
		)

		ch <- prometheus.MustNewConstMetric(
			s.deletion,
			prometheus.CounterValue,
			metrics.TotalDeletions,
			strconv.Itoa(int(vbID)),
		)

		ch <- prometheus.MustNewConstMetric(
			s.expiration,
			prometheus.CounterValue,
			metrics.TotalExpirations,
			strconv.Itoa(int(vbID)),
		)

		return true
	})

	queues := s.client.GetAgentQueues()
	for i := range queues {
		queue := queues[i]

		ch <- prometheus.MustNewConstMetric(
			s.agentQueueCurrent,
			prometheus.GaugeValue,
			float64(queue.Current),
			queue.Address,
			strconv.FormatBool(queue.IsDcp),
		)

		ch <- prometheus.MustNewConstMetric(
			s.agentQueueMax,
			prometheus.GaugeValue,
			float64(queue.Max),
			queue.Address,
			strconv.FormatBool(queue.IsDcp),
		)
	}

	offsets, _, _ := s.stream.GetOffsets()

	var totalLag float64

	offsets.Range(func(vbID uint16, offset *models.Offset) bool {
		ch <- prometheus.MustNewConstMetric(
			s.currentSeqNo,
			prometheus.GaugeValue,
			float64(offset.SeqNo),
			strconv.Itoa(int(vbID)),
		)

		ch <- prometheus.MustNewConstMetric(
			s.startSeqNo,
			prometheus.GaugeValue,
			float64(offset.StartSeqNo),
			strconv.Itoa(int(vbID)),
		)

		ch <- prometheus.MustNewConstMetric(
			s.endSeqNo,
			prometheus.GaugeValue,
			float64(offset.EndSeqNo),
			strconv.Itoa(int(vbID)),
		)

		if err != nil {
			ch <- prometheus.NewInvalidMetric(
				s.lag,
				err,
			)
		} else {
			var lag float64

			seqNo, _ := seqNoMap.Load(vbID)

			if seqNo > offset.SeqNo {
				lag = float64(seqNo - offset.SeqNo)
			}

			totalLag += lag

			ch <- prometheus.MustNewConstMetric(
				s.lag,
				prometheus.GaugeValue,
				lag,
				strconv.Itoa(int(vbID)),
			)
		}

		return true
	})

	ch <- prometheus.MustNewConstMetric(
		s.totalLag,
		prometheus.GaugeValue,
		totalLag,
		[]string{}...,
	)

	streamMetric, activeStream := s.stream.GetMetric()

	ch <- prometheus.MustNewConstMetric(
		s.activeStream,
		prometheus.GaugeValue,
		float64(activeStream),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.processLatency,
		prometheus.GaugeValue,
		float64(streamMetric.ProcessLatency),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.dcpLatency,
		prometheus.CounterValue,
		float64(streamMetric.DcpLatency),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.rebalance,
		prometheus.CounterValue,
		float64(streamMetric.Rebalance),
		[]string{}...,
	)

	vBucketDiscoveryMetric := s.vBucketDiscovery.GetMetric()

	ch <- prometheus.MustNewConstMetric(
		s.totalMembers,
		prometheus.GaugeValue,
		float64(vBucketDiscoveryMetric.TotalMembers),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.memberNumber,
		prometheus.GaugeValue,
		float64(vBucketDiscoveryMetric.MemberNumber),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.membershipType,
		prometheus.GaugeValue,
		0,
		[]string{vBucketDiscoveryMetric.Type}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.vBucketCount,
		prometheus.GaugeValue,
		float64(vBucketDiscoveryMetric.VBucketCount),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.vBucketRangeStart,
		prometheus.GaugeValue,
		float64(vBucketDiscoveryMetric.VBucketRangeStart),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.vBucketRangeEnd,
		prometheus.GaugeValue,
		float64(vBucketDiscoveryMetric.VBucketRangeEnd),
		[]string{}...,
	)

	checkpointMetric := s.stream.GetCheckpointMetric()

	ch <- prometheus.MustNewConstMetric(
		s.offsetWrite,
		prometheus.GaugeValue,
		float64(checkpointMetric.OffsetWrite),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.offsetWriteLatency,
		prometheus.GaugeValue,
		float64(checkpointMetric.OffsetWriteLatency),
		[]string{}...,
	)
}

//nolint:funlen
func NewMetricCollector(client couchbase.Client, stream stream.Stream, vBucketDiscovery stream.VBucketDiscovery) *metricCollector {
	return &metricCollector{
		stream:           stream,
		client:           client,
		vBucketDiscovery: vBucketDiscovery,

		mutation: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "mutation", "total"),
			"Mutation count",
			[]string{"vbId"},
			nil,
		),
		deletion: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "deletion", "total"),
			"Deletion count",
			[]string{"vbId"},
			nil,
		),
		expiration: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "expiration", "total"),
			"Expiration count",
			[]string{"vbId"},
			nil,
		),
		agentQueueCurrent: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "agent_queue", "current"),
			"Client queue current",
			[]string{"address", "is_dcp"},
			nil,
		),
		agentQueueMax: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "agent_queue", "max"),
			"Client queue max",
			[]string{"address", "is_dcp"},
			nil,
		),
		currentSeqNo: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "seq_no", "current"),
			"Current seq no",
			[]string{"vbId"},
			nil,
		),
		startSeqNo: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "start_seq_no", "current"),
			"Start seq no",
			[]string{"vbId"},
			nil,
		),
		endSeqNo: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "end_seq_no", "current"),
			"End seq no",
			[]string{"vbId"},
			nil,
		),
		persistSeqNo: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "persist_seq_no", "current"),
			"Persist seq no",
			[]string{"vbId"},
			nil,
		),
		lag: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "lag", "current"),
			"Lag",
			[]string{"vbId"},
			nil,
		),
		totalLag: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "total_lag", "current"),
			"Total Lag",
			[]string{},
			nil,
		),
		processLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "process_latency_ms", "current"),
			"Average process latency ms",
			[]string{},
			nil,
		),
		dcpLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "dcp_latency_ms", "current"),
			"Latest consumed dcp message latency ms",
			[]string{},
			nil,
		),
		rebalance: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "rebalance", "current"),
			"Rebalance count",
			[]string{},
			nil,
		),
		activeStream: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "active_stream", "current"),
			"Active stream",
			[]string{},
			nil,
		),
		totalMembers: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "total_members", "current"),
			"Total members",
			[]string{},
			nil,
		),
		memberNumber: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "member_number", "current"),
			"Member number",
			[]string{},
			nil,
		),
		membershipType: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "membership_type", "current"),
			"Membership type",
			[]string{"type"},
			nil,
		),
		vBucketCount: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "vbucket_count", "current"),
			"VBucket count",
			[]string{},
			nil,
		),
		vBucketRangeStart: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "vbucket_range_start", "current"),
			"VBucket range start",
			[]string{},
			nil,
		),
		vBucketRangeEnd: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "vbucket_range_end", "current"),
			"VBucket range end",
			[]string{},
			nil,
		),
		offsetWrite: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "offset_write", "current"),
			"Average offset write",
			[]string{},
			nil,
		),
		offsetWriteLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "offset_write_latency_ms", "current"),
			"Average offset write latency ms",
			[]string{},
			nil,
		),
	}
}
