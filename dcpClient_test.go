package main

import (
	"fmt"
	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/assert"
	"go-dcp-client/config"
	"testing"
	"time"
)

func TestInitFromYml(t *testing.T) {
	// todo run in test container
	InitFromYml("config/test.yml", func(mutation Mutation) {
		println("mutated %s", mutation.Key)
		assert.Equal(t, mutation.Key, "my_key")
	}, func(deletion Deletion) {
		println("Deleted %s", deletion.Key)
	}, nil)

	cbSet(agents.opAgent)
}

func TestInit(t *testing.T) {
	// todo run in test container
	Init(config.CouchbaseDCPConfig{
		// todo config
	}, func(mutation Mutation) {
		println("mutated %s", mutation.Key)
		assert.Equal(t, mutation.Key, "my_key")
	}, func(deletion Deletion) {
		println("Deleted %s", deletion.Key)
	}, nil)

	cbSet(agents.opAgent)
}

func cbSet(agent *gocbcore.Agent) error {

	key := "my_key"
	value := "Some value"

	_, err := agent.Set(gocbcore.SetOptions{
		Key:      []byte(key),
		Value:    []byte(value),
		Flags:    0,
		Datatype: 0,
	}, func(result *gocbcore.StoreResult, err error) {
		if err != nil {
			fmt.Errorf("got error %v", err)
		}
		fmt.Printf("Upserted to VB: %d\n", result.MutationToken.VbID)
	})

	println("Done a set?")
	time.Sleep(500 * time.Millisecond)
	return err
}
