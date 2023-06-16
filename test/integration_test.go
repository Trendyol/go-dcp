package main

import (
	"github.com/Trendyol/go-dcp-client/config"
	"github.com/Trendyol/go-dcp-client/couchbase"
	"testing"
	"time"
)

func TestCouchbase(t *testing.T) {
	// Given
	time.Sleep(2 * time.Minute)

	cbClient := couchbase.NewClient(&config.Dcp{
		Hosts:      []string{"localhost:8091"},
		Username:   "admin",
		Password:   "password",
		BucketName: "dcp-test",
	})

	// When
	err := cbClient.Connect()

	// Then
	if err != nil {
		t.Fatalf("err pinging couchbase %s", err.Error())
	}
}
