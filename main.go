package main

import (
	"fmt"
	"github.com/couchbase/gocbcore/v10"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func listener(event int, data interface{}, err error) {
	if err != nil {
		return
	}

	if event == MutationName {
		mutation := data.(gocbcore.DcpMutation)
		fmt.Printf("mutated | id: %v, value: %v\n", string(mutation.Key), string(mutation.Value))
	} else if event == DeletionName {
		deletion := data.(gocbcore.DcpDeletion)
		fmt.Printf("deleted | id: %v, value: %v\n", string(deletion.Key), string(deletion.Value))
	} else if event == ExpirationName {
		expiration := data.(gocbcore.DcpExpiration)
		fmt.Printf("expired | id: %v", string(expiration.Key))
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

	stream := NewStreamWithListener(client, &cbMetadata{agent: *client.GetAgent()}, listener)
	stream.Start()

	cancelChan := make(chan os.Signal, 1)
	signal.Notify(cancelChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		stream.Wait()
	}()
	<-cancelChan
	stream.SaveCheckpoint()
}
