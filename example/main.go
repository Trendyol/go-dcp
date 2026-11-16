package main

import (
	"github.com/Trendyol/go-dcp"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
)

func listener(ctx *models.ListenerContext) {
	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		logger.Log.Info(
			"document mutated",
			"vbID", event.VbID,
			"eventTime", event.EventTime.UnixMilli(),
			"id", string(event.Key),
			"valueLength", len(event.Value),
			"created", event.IsCreated(),
		)
	case models.DcpDeletion:
		logger.Log.Info(
			"document deleted",
			"vbID", event.VbID,
			"eventTime", event.EventTime.UnixMilli(),
			"id", string(event.Key),
		)
	case models.DcpExpiration:
		logger.Log.Info(
			"document expired",
			"vbID", event.VbID,
			"eventTime", event.EventTime.UnixMilli(),
			"id", string(event.Key),
		)
	}

	ctx.Ack()
}

func main() {
	connector, err := dcp.NewDcp("config.yml", listener)
	if err != nil {
		panic(err)
	}

	defer connector.Close()

	connector.Start()
}
