package main

import (
	"context"
	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"
	"testing"
	"time"
)

func setupSuite(tb testing.TB, config Config) func(tb testing.TB) {
	ctx := context.Background()

	couchbaseContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "docker.io/trendyoltech/couchbase-testcontainer:6.5.1",
			ExposedPorts: []string{"8091:8091/tcp", "8093:8093/tcp", "11210:11210/tcp"},
			WaitingFor:   wait.ForLog("/entrypoint.sh couchbase-server").WithStartupTimeout(30 * time.Second),
			Env:          map[string]string{"USERNAME": config.Username, "PASSWORD": config.Password, "BUCKET_NAME": config.BucketName},
		},
		Started: true,
	})

	if err != nil {
		tb.Error(err)
	}

	return func(tb testing.TB) {
		defer couchbaseContainer.Terminate(ctx)
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
	config := NewConfig("configs/test.yml")

	teardownSuite := setupSuite(t, config)
	defer teardownSuite(t)

	client := NewClient(config)

	_ = client.DcpConnect(time.Now().Add(10 * time.Second))

	defer client.DcpClose()

	_ = client.Connect(time.Now().Add(10 * time.Second))

	defer client.Close()

	agent := client.GetAgent()

	insertData(agent)

	metadata := NewFileMetadata("checkpoints.json")

	stream := NewStream(client, metadata)
	stream.Start()
	stream.AddListener(mutationListenerFactory(t, "my_key", stream))
	stream.Wait()
	os.Remove("checkpoints.json")
}

func insertData(agent *gocbcore.Agent) {
	opm := newAsyncOp(nil)
	op, err := agent.Set(gocbcore.SetOptions{
		Key:   []byte("my_key"),
		Value: []byte("Some value"),
	}, func(result *gocbcore.StoreResult, err error) {
		opm.Resolve()
	})

	_ = opm.Wait(op, err)
}
