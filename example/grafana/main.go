package main

import (
	"fmt"

	"github.com/Trendyol/go-dcp"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
	"github.com/couchbase/gocb/v2"
	"time"
)

func listener(ctx *models.ListenerContext) {
	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		logger.Log.Info(
			"document mutated",
			"vbID", event.VbID,
			"eventTime", event.EventTime,
			"id", string(event.Key),
			"value", string(event.Value),
			"created", event.IsCreated(),
		)
	case models.DcpDeletion:
		logger.Log.Info(
			"document deleted",
			"vbID", event.VbID,
			"eventTime", event.EventTime,
			"id", string(event.Key),
		)
	case models.DcpExpiration:
		logger.Log.Info(
			"document expired",
			"vbID", event.VbID,
			"eventTime", event.EventTime,
			"id", string(event.Key),
		)
	}

	ctx.Ack()
}

func main() {
	time.Sleep(15 * time.Second) //wait for couchbase container initialize

	go seedCouchbaseBucket()

	connector, err := dcp.NewDcp("config.yml", listener)
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}

func seedCouchbaseBucket() {
	cluster, err := gocb.Connect("couchbase://couchbase", gocb.ClusterOptions{
		Username: "user",
		Password: "password",
	})
	if err != nil {
		logger.Log.Error("couchbase connect failed", "error", err)
		panic(err)
	}

	bucket := cluster.Bucket("dcp-test")
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		logger.Log.Error("bucket not ready", "error", err)
		panic(err)
	}

	collection := bucket.DefaultCollection()

	counter := 0
	for {
		counter++
		documentID := fmt.Sprintf("doc-%d", counter)
		document := map[string]interface{}{
			"counter": counter,
			"message": "Hello Couchbase",
			"time":    time.Now().Format(time.RFC3339),
		}
		_, err := collection.Upsert(documentID, document, nil)
		if err != nil {
			logger.Log.Error("error inserting document", "id", documentID, "error", err)
		} else {
			logger.Log.Info("inserted document", "id", documentID)
		}
		time.Sleep(1 * time.Second)
	}
}
