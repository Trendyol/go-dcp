package godcpclient

import (
	"context"
	"fmt"
	"github.com/Trendyol/go-dcp-client/models"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/couchbase"
	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/couchbase/gocbcore/v10"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var configStr = `hosts:
  - localhost:8091
username: user
password: password
bucketName: dcp-test
dcp:
  group:
    name: groupName
    membership:
      rebalanceDelay: 5s`

func setupContainer(ctx context.Context, config *helpers.Config) (testcontainers.Container, error) {
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "docker.io/trendyoltech/couchbase-testcontainer:6.5.1",
			ExposedPorts: []string{"8091:8091/tcp", "8093:8093/tcp", "11210:11210/tcp"},
			WaitingFor:   wait.ForLog("/entrypoint.sh couchbase-server").WithStartupTimeout(20 * time.Second),
			Env: map[string]string{
				"USERNAME":       config.Username,
				"PASSWORD":       config.Password,
				"BUCKET_NAME":    config.BucketName,
				"BUCKET_RAMSIZE": "512",
			},
		},
		Started: true,
	})
}

func insertDataToContainer(b *testing.B, mockDataSize int, config *helpers.Config) {
	logger.Log.Printf("mock data stream started with totalSize=%v", mockDataSize)

	client := couchbase.NewClient(config)

	err := client.Connect()
	if err != nil {
		b.Error(err)
	}

	ids := make([]int, mockDataSize)
	for i := range ids {
		ids[i] = i
	}

	// 2048 is the default value for the max queue size of the client, so we need to make sure that we don't exceed that
	chunks := helpers.ChunkSlice[int](ids, int(math.Ceil(float64(mockDataSize)/float64(2048))))

	// Concurrency is limited to 24 to avoid server overload
	iterations := helpers.ChunkSlice(chunks, int(math.Ceil(float64(len(chunks))/float64(16))))

	for _, iteration := range iterations {
		for _, chunk := range iteration {
			wg := &sync.WaitGroup{}
			wg.Add(len(chunk))

			for _, id := range chunk {
				go func(i int) {
					ch := make(chan error)

					opm := couchbase.NewAsyncOp(context.Background())

					op, err := client.GetAgent().Set(gocbcore.SetOptions{
						Key:   []byte(fmt.Sprintf("%v", i)),
						Value: []byte(fmt.Sprintf("%v", i)),
					}, func(result *gocbcore.StoreResult, err error) {
						opm.Resolve()

						ch <- err
					})

					err = opm.Wait(op, err)

					if err != nil {
						b.Error(err)
					}

					err = <-ch

					if err != nil {
						b.Error(err)
					}

					wg.Done()
				}(id)
			}

			wg.Wait()
		}
	}

	client.Close()

	logger.Log.Printf("mock data stream finished with totalSize=%v", mockDataSize)
}

func dcpBench(mockDataSize int) {
	totalNotify := 10
	notifySize := mockDataSize / totalNotify

	ctx := context.Background()

	configFile, err := helpers.CreateConfigFile(configStr)
	if err != nil {
		b.Error(err)
	}
	configPath := configFile.Name()
	config := helpers.NewConfig(fmt.Sprintf("%v_data_insert", helpers.Name), configPath)

	container, err := setupContainer(ctx, config)
	if err != nil {
		b.Error(err)
	}

	counter := 0
	finish := make(chan struct{}, 1)

	dcp, err := NewDcp(configPath, func(ctx *models.ListenerContext) {
		if _, ok := ctx.Event.(models.DcpMutation); ok {
			if counter == 0 {
				b.ResetTimer()
			}

			ctx.Ack()

			counter++

			if counter%notifySize == 0 {
				logger.Log.Printf("%v/%v processed", counter/notifySize, totalNotify)
			}

			if counter == mockDataSize {
				finish <- struct{}{}
			}
		}
	})
	if err != nil {
		b.Error(err)
	}

	go func() {
		<-dcp.WaitUntilReady()
		insertDataToContainer(b, mockDataSize, config)
	}()

	go func() {
		<-finish
		dcp.Close()
	}()

	dcp.Start()

	err = container.Terminate(ctx)
	if err != nil {
		b.Error(err)
	}

	err = configFile.Close()
	if err != nil {
		b.Error(err)
	}

	err = os.Remove(configPath)
	if err != nil {
		b.Error(err)
	}
}

//nolint:funlen
func BenchmarkDcp(b *testing.B) {
	b.Run("Dcp(640000)", func(b *testing.B) {
		dcpBench(640000)
	})

	b.Run("Dcp(1280000)", func(b *testing.B) {
		dcpBench(1280000)
	})
}
