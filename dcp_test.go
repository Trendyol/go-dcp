package godcpclient

import (
	"context"
	"fmt"
	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func createConfigFile(t *testing.T) (string, func()) {
	configStr := `couchbase:
  hosts:
    - localhost:8091
  username: Administrator
  password: password
  bucketName: sample
  userAgent: unit-test-listener
  compression: true
  metadataBucket: sample
  connectTimeout: 10000
  dcp:
    connectTimeout: 10000
    flowControlBuffer: 16
    group:
      name: groupName
      membership:
        memberNumber: 1
        totalMembers: 1`

	tmpFile, err := os.CreateTemp("", "*.yml")

	if err != nil {
		t.Error(err)
	}

	if _, err = tmpFile.WriteString(configStr); err != nil {
		t.Error(err)
	}

	return tmpFile.Name(), func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}
}

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
	configPath, configFileClean := createConfigFile(t)
	defer configFileClean()

	config := NewConfig(configPath)

	containerShutdown := setupContainer(t, config)
	defer containerShutdown()

	insertDataToContainer(t, config)

	var dcp Dcp
	var err error

	counter := 0
	lock := sync.Mutex{}

	dcp, err = NewDcp(configPath, func(event interface{}, err error) {
		if err != nil {
			return
		}

		if event, ok := event.(DcpMutation); ok {
			lock.Lock()
			counter++
			lock.Unlock()

			assert.True(t, strings.HasPrefix(string(event.Key), "my_key"))
			assert.True(t, strings.HasPrefix(string(event.Value), "my_value"))

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
