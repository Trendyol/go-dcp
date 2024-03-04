package main

import (
	"context"
	"github.com/Trendyol/go-dcp"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func listener(ctx *models.ListenerContext) {
	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		logger.Log.Info(
			"mutated(vb=%v,eventTime=%v) | id: %v, value: %v | isCreated: %v",
			event.VbID, event.EventTime, string(event.Key), string(event.Value), event.IsCreated(),
		)
	case models.DcpDeletion:
		logger.Log.Info(
			"deleted(vb=%v,eventTime=%v) | id: %v",
			event.VbID, event.EventTime, string(event.Key),
		)
	case models.DcpExpiration:
		logger.Log.Info(
			"expired(vb=%v,eventTime=%v) | id: %v",
			event.VbID, event.EventTime, string(event.Key),
		)
	}

	ctx.Ack()
}

func main() {
	ctx := context.Background()
	exporter := tracetest.NewInMemoryExporter()
	defer exporter.Shutdown(ctx)
	bsp := sdktrace.NewSimpleSpanProcessor(exporter)
	defer bsp.Shutdown(ctx)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(bsp))
	defer tp.Shutdown(ctx)
	otel.SetTracerProvider(tp)

	connector, err := dcp.NewDcp("../config.yml", listener, NewOpenTelemetryRequestTracer(tp))
	if err != nil {
		panic(err)
	}

	defer connector.Close()

	connector.Start()
}
