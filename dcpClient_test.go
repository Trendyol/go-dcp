package main

import (
	"context"
	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go-dcp-client/config"
	"log"
	"testing"
	"time"
)

const defaultUser = "Administrator"
const defaultPassword = "password"
const defaultBucket = "Sample"

// You can use testing.T, if you want to test the code without benchmarking
func setupSuite(tb testing.TB) func(tb testing.TB) {
	ctx := context.Background()

	// Spin up Etcd

	// ref: https://github.com/Trendyol/couchbase-docker-image-for-testcontainers, https://medium.com/trendyol-tech/couchbase-integration-testing-for-golang-via-testcontainers-25b906bc2c0d
	couchbaseContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "docker.io/trendyoltech/couchbase-testcontainer:6.5.1",
			ExposedPorts: []string{"8091:8091/tcp", "8093:8093/tcp", "11210:11210/tcp"},
			WaitingFor:   wait.ForLog("couchbase-dev started").WithStartupTimeout(45 * time.Second),
			Env:          map[string]string{"USERNAME": defaultUser, "PASSWORD": defaultPassword, "BUCKET_NAME": defaultBucket},
		},
		Started: true,
	})

	if err != nil {
		tb.Error(err)
	}
	// ip, err := couchbaseContainer.Host(ctx)
	// if err != nil {
	// tb.Error(err)
	// }

	// Return a function to teardown the test
	return func(tb testing.TB) {
		defer couchbaseContainer.Terminate(ctx)
		log.Println("teardown suite")
	}
}

func TestInit(t *testing.T) {
	teardownSuite := setupSuite(t)
	defer teardownSuite(t)

	cbSet()

	mutationCount := 0
	Init(config.CouchbaseDCPConfig{
		Hosts:      []string{"localhost"},
		Username:   defaultUser,
		Password:   defaultPassword,
		BucketName: defaultBucket,
		Dcp: config.CouchbaseDCPConfigDCP{
			MetadataBucket: defaultBucket,
			Compression:    true,
			Group: config.CouchbaseDCPConfigDCPGroup{
				Name: "test-group",
			},
			FlowControlBuffer:          0,
			PersistencePollingInterval: 0,
		},
	}, func(mutation Mutation) {
		println("mutated %s", mutation.Key)
		mutationCount++
		assert.Equal(t, mutation.Key, "my_key")
	}, func(deletion Deletion) {
		println("Deleted %s", deletion.Key)
	}, nil)

	assert.Equal(t, 1, mutationCount)
}

func cbSet() error {
	agentConfig := gocbcore.AgentConfig{
		UserAgent:  "go-dcp-client",
		BucketName: defaultBucket,
		SeedConfig: gocbcore.SeedConfig{
			HTTPAddrs: []string{"localhost"},
		},
		SecurityConfig: gocbcore.SecurityConfig{
			Auth: gocbcore.PasswordAuthProvider{
				Username: defaultUser,
				Password: defaultPassword,
			},
		},
	}

	agent, err := gocbcore.CreateAgent(&agentConfig)
	defer agent.Close()
	if err != nil {
		log.Printf("creating set operation agent error %v", err)
	}

	key := "my_key"
	value := "Some value"

	_, err = agent.Set(gocbcore.SetOptions{
		Key:      []byte(key),
		Value:    []byte(value),
		Flags:    0,
		Datatype: 0,
	}, func(result *gocbcore.StoreResult, err error) {
		if err != nil {
			log.Printf("got error %v", err)
		}
		log.Printf("Upserted to VB: %v\n", result)
	})

	if err != nil {
		log.Printf("set operation error %v", err)
	}

	return err
}
