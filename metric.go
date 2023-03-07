package godcpclient

import (
	"strconv"

	gDcp "github.com/Trendyol/go-dcp-client/dcp"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/ansrivas/fiberprometheus/v2"
	"github.com/gofiber/fiber/v2"

	"github.com/prometheus/client_golang/prometheus"
)

type metricCollector struct {
	stream Stream
	client gDcp.Client

	mutation   *prometheus.Desc
	deletion   *prometheus.Desc
	expiration *prometheus.Desc

	currentSeqNo *prometheus.Desc
	startSeqNo   *prometheus.Desc
	endSeqNo     *prometheus.Desc

	averageProcessMs *prometheus.Desc

	lag *prometheus.Desc
}

func (s *metricCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(s, ch)
}

//nolint:funlen
func (s *metricCollector) Collect(ch chan<- prometheus.Metric) {
	seqNoMap, err := s.client.GetVBucketSeqNos()
	if err != nil {
		logger.Error(err, "cannot get seqNoMap")
	}

	for vbID, observer := range s.stream.GetObservers() {
		metric := observer.GetMetric()

		ch <- prometheus.MustNewConstMetric(
			s.mutation,
			prometheus.CounterValue,
			metric.TotalMutations,
			strconv.Itoa(int(vbID)),
		)

		ch <- prometheus.MustNewConstMetric(
			s.deletion,
			prometheus.CounterValue,
			metric.TotalDeletions,
			strconv.Itoa(int(vbID)),
		)

		ch <- prometheus.MustNewConstMetric(
			s.expiration,
			prometheus.CounterValue,
			metric.TotalExpirations,
			strconv.Itoa(int(vbID)),
		)
	}

	s.stream.LockOffsets()
	defer s.stream.UnlockOffsets()

	for vbID, offset := range s.stream.GetOffsets() {
		ch <- prometheus.MustNewConstMetric(
			s.currentSeqNo,
			prometheus.CounterValue,
			float64(offset.SeqNo),
			strconv.Itoa(int(vbID)),
		)

		ch <- prometheus.MustNewConstMetric(
			s.startSeqNo,
			prometheus.CounterValue,
			float64(offset.StartSeqNo),
			strconv.Itoa(int(vbID)),
		)

		ch <- prometheus.MustNewConstMetric(
			s.endSeqNo,
			prometheus.CounterValue,
			float64(offset.EndSeqNo),
			strconv.Itoa(int(vbID)),
		)

		var endSeqNo uint64

		if seqNoMap == nil {
			endSeqNo = offset.EndSeqNo
		} else {
			endSeqNo = seqNoMap[vbID]
		}

		ch <- prometheus.MustNewConstMetric(
			s.lag,
			prometheus.CounterValue,
			float64(endSeqNo-offset.SeqNo),
			strconv.Itoa(int(vbID)),
		)
	}

	streamMetric := s.stream.GetMetric()

	ch <- prometheus.MustNewConstMetric(
		s.averageProcessMs,
		prometheus.CounterValue,
		streamMetric.AverageTookMs.Value(),
		[]string{}...,
	)
}

func NewMetricMiddleware(app *fiber.App, config helpers.Config, stream Stream, client gDcp.Client) (func(ctx *fiber.Ctx) error, error) {
	err := prometheus.DefaultRegisterer.Register(&metricCollector{
		stream: stream,
		client: client,

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
		lag: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "lag", "current"),
			"Lag",
			[]string{"vbId"},
			nil,
		),
		averageProcessMs: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "average_process_ms", "current"),
			"Average process ms at 10sec windows",
			[]string{},
			nil,
		),
	})
	if err != nil {
		return nil, err
	}

	fiberPrometheus := fiberprometheus.New(config.Dcp.Group.Name)
	fiberPrometheus.RegisterAt(app, config.Metric.Path)

	logger.Info("metric middleware registered on path %s", config.Metric.Path)

	return fiberPrometheus.Middleware, nil
}
