package main

import (
	"log"
	mathRand "math/rand"
	"time"

	"github.com/Trendyol/go-dcp-client"

	"github.com/Trendyol/go-dcp-client/models"
)

func listener(ctx *models.ListenerContext) {
	time.Sleep(time.Duration(mathRand.Intn(100)) * time.Millisecond) // simulate some work

	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		log.Printf("mutated(%v) | id: %v, value: %v | isCreated: %v", event.VbID, string(event.Key), string(event.Value), event.IsCreated())
	case models.DcpDeletion:
		log.Printf("deleted | id: %v", string(event.Key))
	case models.DcpExpiration:
		log.Printf("expired | id: %v", string(event.Key))
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
