package main

import (
	"context"
	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
	"time"
)

func setupContainer(t *testing.T, config Config) func() {
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "docker.io/trendyoltech/couchbase-testcontainer:6.5.1",
			ExposedPorts: []string{"8091:8091/tcp", "8093:8093/tcp", "11210:11210/tcp"},
			WaitingFor:   wait.ForLog("/entrypoint.sh couchbase-server").WithStartupTimeout(30 * time.Second),
			Env:          map[string]string{"USERNAME": config.Username, "PASSWORD": config.Password, "BUCKET_NAME": config.BucketName},
		},
		Started: true,
	})

	if err != nil {
		t.Error(err)
	}

	return func() {
		defer container.Terminate(ctx)
	}
}

func insertDataToContainer(t *testing.T, config Config) {
	client := NewClient(config)

	_ = client.Connect()
	defer client.Close()

	opm := newAsyncOp(nil)
	op, err := client.GetAgent().Set(gocbcore.SetOptions{
		Key:   []byte("my_key"),
		Value: []byte("Some value"),
	}, func(result *gocbcore.StoreResult, err error) {
		opm.Resolve()
	})

	if err != nil {
		t.Error(err)
	}

	err = opm.Wait(op, err)

	if err != nil {
		t.Error(err)
	}
}

func TestDcp(t *testing.T) {
	configPath := "configs/test.yml"
	config := NewConfig(configPath)

	containerShutdown := setupContainer(t, config)
	defer containerShutdown()

	insertDataToContainer(t, config)

	var dcp Dcp
	var err error

	dcp, err = NewDcp(configPath, func(event int, data interface{}, err error) {
		if err != nil {
			return
		}

		if event == MutationName {
			mutation := data.(gocbcore.DcpMutation)
			assert.Equal(t, string(mutation.Key), "my_key")
			dcp.Close()
		}
	})

	if err != nil {
		t.Error(err)
	}

	dcp.StartAndWait()
}
