package godcpclient

import (
	"github.com/ansrivas/fiberprometheus/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus"
	"log"
	"strconv"
)

type MetricCollector struct {
	observer Observer

	mutation   *prometheus.Desc
	deletion   *prometheus.Desc
	expiration *prometheus.Desc

	currentSeqNo *prometheus.Desc
	startSeqNo   *prometheus.Desc
	endSeqNo     *prometheus.Desc
}

func (s *MetricCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(s, ch)
}

func (s *MetricCollector) Collect(ch chan<- prometheus.Metric) {
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

	for vbId, state := range s.observer.GetState() {
		ch <- prometheus.MustNewConstMetric(
			s.currentSeqNo,
			prometheus.CounterValue,
			float64(state.SeqNo),
			strconv.Itoa(int(vbId)),
		)

		ch <- prometheus.MustNewConstMetric(
			s.startSeqNo,
			prometheus.CounterValue,
			float64(state.StartSeqNo),
			strconv.Itoa(int(vbId)),
		)

		ch <- prometheus.MustNewConstMetric(
			s.endSeqNo,
			prometheus.CounterValue,
			float64(state.EndSeqNo),
			strconv.Itoa(int(vbId)),
		)
	}
}

func NewMetricMiddleware(app *fiber.App, config Config, observer Observer) func(ctx *fiber.Ctx) error {
	err := prometheus.DefaultRegisterer.Register(&MetricCollector{
		observer: observer,

		mutation: prometheus.NewDesc(
			prometheus.BuildFQName(Name, "mutation", "total"),
			"Mutation count",
			nil,
			nil,
		),
		deletion: prometheus.NewDesc(
			prometheus.BuildFQName(Name, "deletion", "total"),
			"Deletion count",
			nil,
			nil,
		),
		expiration: prometheus.NewDesc(
			prometheus.BuildFQName(Name, "expiration", "total"),
			"Expiration count",
			nil,
			nil,
		),
		currentSeqNo: prometheus.NewDesc(
			prometheus.BuildFQName(Name, "seq_no", "current"),
			"Current seq no",
			[]string{"vbId"},
			nil,
		),
		startSeqNo: prometheus.NewDesc(
			prometheus.BuildFQName(Name, "start_seq_no", "current"),
			"Start seq no",
			[]string{"vbId"},
			nil,
		),
		endSeqNo: prometheus.NewDesc(
			prometheus.BuildFQName(Name, "end_seq_no", "current"),
			"End seq no",
			[]string{"vbId"},
			nil,
		),
	})

	if err != nil {
		panic(err)
	}

	fiberPrometheus := fiberprometheus.New(config.UserAgent)
	fiberPrometheus.RegisterAt(app, config.Metric.Path)

	log.Printf("Metric middleware registered on path %s", config.Metric.Path)

	return fiberPrometheus.Middleware
}
