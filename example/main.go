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
			"mutated(vb=%v,eventTime=%v) | id: %v, value len: %v | isCreated: %v",
			event.VbID, event.EventTime.UnixMilli(), string(event.Key), len(event.Value), event.IsCreated(),
		)
	case models.DcpDeletion:
		logger.Log.Info(
			"deleted(vb=%v,eventTime=%v) | id: %v",
			event.VbID, event.EventTime.UnixMilli(), string(event.Key),
		)
	case models.DcpExpiration:
		logger.Log.Info(
			"expired(vb=%v,eventTime=%v) | id: %v",
			event.VbID, event.EventTime.UnixMilli(), string(event.Key),
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
