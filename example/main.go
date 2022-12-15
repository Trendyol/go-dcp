package main

import (
	"github.com/Trendyol/go-dcp-client"
	"log"
)

type listener struct {
}

func (l *listener) DcpMutation() {

}

func listener(event interface{}, err error) {
	if err != nil {
		log.Printf("error | %v", err)
		return
	}

	switch event := event.(type) {
	case godcpclient.DcpScopeCreation:
		log.Printf("mutated | id: %v, value: %v", string(event.Key), string(event.Value))
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

	go func() {
		dcp.Start()
		dcp.Close()
	}()

}
