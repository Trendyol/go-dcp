package main

import (
	"log"

	"github.com/Trendyol/go-dcp-client"
)

func listener(event interface{}, err error) {
	if err != nil {
		log.Printf("error | %v", err)
		return
	}

	switch event := event.(type) {
	case godcpclient.DcpMutation:
		log.Printf("mutated | id: %v, value: %v | isCreated: %v", string(event.Key), string(event.Value), event.IsCreated())
	case godcpclient.DcpDeletion:
		log.Printf("deleted | id: %v", string(event.Key))
	case godcpclient.DcpExpiration:
		log.Printf("expired | id: %v", string(event.Key))
	}
}

func main() {
	dcp, err := godcpclient.NewDcp("config.yml", listener)
	if err != nil {
		panic(err)
	}

	defer dcp.Close()

	dcp.Start()
}
