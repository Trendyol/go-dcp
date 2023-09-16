package main

import (
	"github.com/Trendyol/go-dcp"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
)

func listener(ctx *models.ListenerContext) {
	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		logger.Log.Debug(
			"mutated(vb=%v,eventTime=%v) | id: %v, value: %v | isCreated: %v",
			event.VbID, event.EventTime, string(event.Key), string(event.Value), event.IsCreated(),
		)
	case models.DcpDeletion:
		logger.Log.Debug(
			"deleted(vb=%v,eventTime=%v) | id: %v",
			event.VbID, event.EventTime, string(event.Key),
		)
	case models.DcpExpiration:
		logger.Log.Debug(
			"expired(vb=%v,eventTime=%v) | id: %v",
			event.VbID, event.EventTime, string(event.Key),
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
