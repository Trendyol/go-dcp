package main

import (
	"github.com/Trendyol/go-dcp-client"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/models"
)

func listener(ctx *models.ListenerContext) {
	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		logger.Log.Printf(
			"mutated(vb=%v,eventTime=%v) | id: %v, value: %v | isCreated: %v",
			event.VbID, event.EventTime, string(event.Key), string(event.Value), event.IsCreated(),
		)
	case models.DcpDeletion:
		logger.Log.Printf(
			"deleted(vb=%v,eventTime=%v) | id: %v",
			event.VbID, event.EventTime, string(event.Key),
		)
	case models.DcpExpiration:
		logger.Log.Printf(
			"expired(vb=%v,eventTime=%v) | id: %v",
			event.VbID, event.EventTime, string(event.Key),
		)
	}

	ctx.Ack()
}

func main() {
	var dcp godcpclient.Dcp
	for dcp == nil {
		dcp, _ = godcpclient.NewDcp("config.yml", listener)
	}
	defer dcp.Close()

	dcp.Start()
}
