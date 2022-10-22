package main

import (
	. "github.com/Trendyol/go-dcp-client"
	"log"
)

func listener(event interface{}, err error) {
	if err != nil {
		log.Printf("error | %v", err)
		return
	}

	switch event := event.(type) {
	case DcpMutation:
		log.Printf("mutated | id: %v, value: %v", string(event.Key), string(event.Value))
	case DcpDeletion:
		log.Printf("deleted | id: %v, value: %v", string(event.Key), string(event.Value))
	case DcpExpiration:
		log.Printf("expired | id: %v", string(event.Key))
	}
}

func main() {
	dcp, err := NewDcp("config.yml", listener)
	defer dcp.Close()

	if err != nil {
		panic(err)
	}

	dcp.StartAndWait()
}
