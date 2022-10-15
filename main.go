package main

import (
	"fmt"
	"go-dcp-client/config"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func listener(event int, data interface{}, err error) {
	if err != nil {
		return
	}

	fmt.Printf("event: %d, data: %v, err: %v\n", event, data, err)
}

func main() {
	cbConfig := config.LoadConfig("config/test.yml")

	client := NewClient()

	err := client.DcpConnect(
		cbConfig.Hosts,
		cbConfig.Username,
		cbConfig.Password,
		cbConfig.Dcp.Group.Name,
		"go-dcp-client",
		cbConfig.BucketName,
		time.Now().Add(10*time.Second),
		cbConfig.Dcp.Compression,
		cbConfig.Dcp.FlowControlBuffer,
	)

	defer client.DcpClose()

	if err != nil {
		panic(err)
	}

	stream := NewStreamWithListener(client, listener)
	stream.Start()

	cancelChan := make(chan os.Signal, 1)
	signal.Notify(cancelChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		stream.Wait()
	}()
	<-cancelChan
	stream.SaveCheckpoint()
}
