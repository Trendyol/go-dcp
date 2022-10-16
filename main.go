package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func listener(event int, data interface{}, err error) {
	if err != nil {
		return
	}

	if event == MutationName || event == DeletionName || event == ExpirationName {
		fmt.Printf("event: %d, data: %v, err: %v\n", event, data, err)
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
