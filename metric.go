package godcpclient

import (
	"strconv"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/ansrivas/fiberprometheus/v2"
	"github.com/gofiber/fiber/v2"

	"github.com/prometheus/client_golang/prometheus"
)

type metricCollector struct {
	observer Observer

	mutation   *prometheus.Desc
	deletion   *prometheus.Desc
	expiration *prometheus.Desc

	currentSeqNo *prometheus.Desc
	startSeqNo   *prometheus.Desc
	endSeqNo     *prometheus.Desc
}

func (s *metricCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(s, ch)
}

func (s *metricCollector) Collect(ch chan<- prometheus.Metric) {
	if s.observer == nil {
		return
	}

	metric := s.observer.GetMetric()

	ch <- prometheus.MustNewConstMetric(
		s.mutation,
		prometheus.CounterValue,
		metric.TotalMutations,
	)

	ch <- prometheus.MustNewConstMetric(
		s.deletion,
		prometheus.CounterValue,
		metric.TotalDeletions,
	)

	ch <- prometheus.MustNewConstMetric(
		s.expiration,
		prometheus.CounterValue,
		metric.TotalExpirations,
	)

	for vbID, state := range s.observer.GetState() {
		ch <- prometheus.MustNewConstMetric(
			s.currentSeqNo,
			prometheus.CounterValue,
			float64(state.SeqNo),
			strconv.Itoa(int(vbID)),
		)

		ch <- prometheus.MustNewConstMetric(
			s.startSeqNo,
			prometheus.CounterValue,
			float64(state.StartSeqNo),
			strconv.Itoa(int(vbID)),
		)

		ch <- prometheus.MustNewConstMetric(
			s.endSeqNo,
			prometheus.CounterValue,
			float64(state.EndSeqNo),
			strconv.Itoa(int(vbID)),
		)
	}
}

func NewMetricMiddleware(app *fiber.App, config helpers.Config, observer Observer) (func(ctx *fiber.Ctx) error, error) {
	err := prometheus.DefaultRegisterer.Register(&metricCollector{
		observer: observer,

		mutation: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "mutation", "total"),
			"Mutation count",
			nil,
			nil,
		),
		deletion: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "deletion", "total"),
			"Deletion count",
			nil,
			nil,
		),
		expiration: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "expiration", "total"),
			"Expiration count",
			nil,
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
	})
	if err != nil {
		return nil, err
	}

	fiberPrometheus := fiberprometheus.New(config.Dcp.Group.Name)
	fiberPrometheus.RegisterAt(app, config.Metric.Path)

	logger.Info("metric middleware registered on path %s", config.Metric.Path)

	return fiberPrometheus.Middleware, nil
}
