package main

import (
	"context"
	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"testing"
	"time"
)

const defaultUser = "Administrator"
const defaultPassword = "password"
const defaultBucket = "Sample"

func setupSuite(tb testing.TB) func(tb testing.TB) {
	ctx := context.Background()

	couchbaseContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "docker.io/trendyoltech/couchbase-testcontainer:6.5.1",
			ExposedPorts: []string{"8091:8091/tcp", "8093:8093/tcp", "11210:11210/tcp"},
			WaitingFor:   wait.ForLog("/entrypoint.sh couchbase-server").WithStartupTimeout(30 * time.Second),
			Env:          map[string]string{"USERNAME": defaultUser, "PASSWORD": defaultPassword, "BUCKET_NAME": defaultBucket},
		},
		Started: true,
	})

	if err != nil {
		tb.Error(err)
	}

	return func(tb testing.TB) {
		defer couchbaseContainer.Terminate(ctx)
		log.Println("teardown suite")
	}
}

func mutationListenerFactory(t *testing.T, expectedKey string, stream Stream) Listener {
	return func(event int, data interface{}, err error) {
		if err != nil {
			return
		}

		if event == MutationName {
			mutation := data.(gocbcore.DcpMutation)
			assert.Equal(t, string(mutation.Key), expectedKey)
			stream.Stop()
		}
	}
}

func TestInit(t *testing.T) {
	teardownSuite := setupSuite(t)
	defer teardownSuite(t)

	client := NewClient()

	_ = client.DcpConnect(
		[]string{"localhost:8091"},
		defaultUser,
		defaultPassword,
		"test-group",
		"go-dcp-client",
		defaultBucket,
		time.Now().Add(10*time.Second),
		true,
		0,
	)

	defer client.DcpClose()

	_ = client.Connect(
		[]string{"localhost:8091"},
		defaultUser,
		defaultPassword,
		"go-dcp-client",
		defaultBucket,
		time.Now().Add(10*time.Second),
		true,
	)

	defer client.Close()

	agent := client.GetAgent()

	insertData(agent)

	stream := NewStream(client, &cbMetadata{agent: *agent})
	stream.Start()
	stream.AddListener(mutationListenerFactory(t, "my_key", stream))
	stream.Wait()
}

func insertData(agent *gocbcore.Agent) {
	opm := newAsyncOp(nil)
	op, err := agent.Set(gocbcore.SetOptions{
		Key:   []byte("my_key"),
		Value: []byte("Some value"),
	}, func(result *gocbcore.StoreResult, err error) {
		if err != nil {
			log.Printf("insert data got error %v", err)
		} else {
			log.Printf("insert data success %v\n", result)
		}

		opm.Resolve()
	})

	_ = opm.Wait(op, err)
}
