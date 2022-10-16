package main

import (
	"github.com/couchbase/gocbcore/v10"
	"log"
	"time"
)

func listener(event int, data interface{}, err error) {
	if err != nil {
		return
	}

	if event == MutationName {
		mutation := data.(gocbcore.DcpMutation)
		log.Printf("mutated | id: %v, value: %v", string(mutation.Key), string(mutation.Value))
	} else if event == DeletionName {
		deletion := data.(gocbcore.DcpDeletion)
		log.Printf("deleted | id: %v, value: %v", string(deletion.Key), string(deletion.Value))
	} else if event == ExpirationName {
		expiration := data.(gocbcore.DcpExpiration)
		log.Printf("expired | id: %v", string(expiration.Key))
	}
}

func main() {
	config := NewConfig("configs/main.yml")

	client := NewClient(config)

	err := client.DcpConnect(time.Now().Add(10 * time.Second))
	defer client.DcpClose()

	if err != nil {
		panic(err)
	}

	err = client.Connect(time.Now().Add(10 * time.Second))
	defer client.Close()

	if err != nil {
		panic(err)
	}

	metadata := NewCBMetadata(client.GetAgent())
	stream := NewStream(client, metadata, listener).Start()
	stream.Wait()
}
