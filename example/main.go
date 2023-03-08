package main

import (
	"log"

	"github.com/Trendyol/go-dcp-client"

	"github.com/Trendyol/go-dcp-client/models"
)

func listener(ctx *models.ListenerContext) {
	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		log.Printf("mutated(vb=%v) | id: %v, value: %v | isCreated: %v", event.VbID, string(event.Key), string(event.Value), event.IsCreated())
	case models.DcpDeletion:
		log.Printf("deleted(vb=%v) | id: %v", event.VbID, string(event.Key))
	case models.DcpExpiration:
		log.Printf("expired(vb=%v) | id: %v", event.VbID, string(event.Key))
	}

	ctx.Ack()
}

func main() {
	dcp, err := godcpclient.NewDcp("config.yml", listener)
	if err != nil {
		panic(err)
	}

	defer dcp.Close()

	dcp.Start()
}
