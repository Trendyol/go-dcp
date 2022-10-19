package main

import (
	"context"
	"fmt"
	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"strings"
	"sync"
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

	wg := sync.WaitGroup{}
	wg.Add(1000)

	for i := 0; i < 1000; i++ {
		go func(i int) {
			ch := make(chan error)

			opm := newAsyncOp(nil)
			op, err := client.GetAgent().Set(gocbcore.SetOptions{
				Key:   []byte(fmt.Sprintf("my_key_%v", i)),
				Value: []byte(fmt.Sprintf("my_value_%v", i)),
			}, func(result *gocbcore.StoreResult, err error) {
				ch <- err
				opm.Resolve()
			})

			if err != nil {
				t.Error(err)
			}

			err = <-ch

			err = opm.Wait(op, err)

			if err != nil {
				t.Error(err)
			}

			wg.Done()
		}(i)
	}

	wg.Wait()
}

func TestDcp(t *testing.T) {
	configPath := "configs/test.yml"
	config := NewConfig(configPath)

	containerShutdown := setupContainer(t, config)
	defer containerShutdown()

	insertDataToContainer(t, config)

	var dcp Dcp
	var err error

	counter := 0
	lock := sync.Mutex{}

	dcp, err = NewDcp(configPath, func(event int, data interface{}, err error) {
		if err != nil {
			return
		}

		if event == MutationName {
			lock.Lock()
			counter++
			lock.Unlock()

			mutation := data.(gocbcore.DcpMutation)

			assert.True(t, strings.HasPrefix(string(mutation.Key), "my_key"))
			assert.True(t, strings.HasPrefix(string(mutation.Value), "my_value"))

			if counter == 1000 {
				dcp.Close()
			}
		}
	})

	if err != nil {
		t.Error(err)
	}

	dcp.StartAndWait()
}
