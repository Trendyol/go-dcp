package integration

import (
	"github.com/Trendyol/go-dcp-client/config"
	"github.com/Trendyol/go-dcp-client/couchbase"
	"testing"
	"time"
)

func TestCouchbase(t *testing.T) {
	cbClient := couchbase.NewClient(&config.Dcp{
		Hosts:             []string{"localhost:8091"},
		Username:          "user",
		Password:          "123456",
		BucketName:        "dcp-test",
		ConnectionTimeout: time.Second * 5,
	})

	// When
	err := cbClient.Connect()
	// Then
	if err != nil {
		t.Fatalf("err pinging couchbase %s", err.Error())
	}
	t.Log("done done done")
}
