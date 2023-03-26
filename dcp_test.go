package godcpclient

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp-client/couchbase"
	"github.com/Trendyol/go-dcp-client/models"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/couchbase/gocbcore/v10"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupContainer(b *testing.B, config *helpers.Config) func() {
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "docker.io/trendyoltech/couchbase-testcontainer:6.5.1",
			ExposedPorts: []string{"8091:8091/tcp", "8093:8093/tcp", "11210:11210/tcp"},
			WaitingFor:   wait.ForLog("/entrypoint.sh couchbase-server").WithStartupTimeout(20 * time.Second),
			Env: map[string]string{
				"USERNAME":    config.Username,
				"PASSWORD":    config.Password,
				"BUCKET_NAME": config.BucketName,
			},
		},
		Started: true,
	})
	if err != nil {
		b.Error(err)
	}

	return func() {
		_ = container.Terminate(ctx)
	}
}

func insertDataToContainer(b *testing.B, mockDataSize int, config *helpers.Config) {
	client := couchbase.NewClient(config)

	_ = client.Connect()

	ids := make([]int, mockDataSize)
	for i := range ids {
		ids[i] = i
	}

	// 2048 is the default value for the max queue size of the client, so we need to make sure that we don't exceed that
	chunks := helpers.ChunkSlice[int](ids, int(math.Ceil(float64(mockDataSize)/float64(2048))))

	// Concurrency is limited to 8 to avoid server overload
	iterations := helpers.ChunkSlice(chunks, int(math.Ceil(float64(len(chunks))/float64(8))))

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
		time.Sleep(1 * time.Second)
	}

	client.Close()

	time.Sleep(5 * time.Second)

	log.Printf("Inserted %v items", mockDataSize)
}

func BenchmarkDcp(benchmark *testing.B) {
	mockDataSize := 320000
	saveTarget := mockDataSize / 2

	configPath, configFileClean, err := helpers.CreateConfigFile()
	if err != nil {
		benchmark.Error(err)
	}
	defer configFileClean()

	config := helpers.NewConfig(fmt.Sprintf("%v_data_insert", helpers.Name), configPath)

	containerShutdown := setupContainer(benchmark, config)
	defer containerShutdown()

	insertDataToContainer(benchmark, mockDataSize, config)

	var dcp Dcp

	counter := 0
	finish := make(chan struct{}, 1)

	dcp, err = NewDcp(configPath, func(ctx *models.ListenerContext) {
		if _, ok := ctx.Event.(models.DcpMutation); ok {
			ctx.Ack()

			counter++

			if counter == saveTarget {
				ctx.Commit()
			}

			if counter == mockDataSize {
				finish <- struct{}{}
			}
		}
	})

	if err != nil {
		benchmark.Error(err)
	}

	go func() {
		<-finish
		dcp.Close()
	}()

	dcp.Start()
}
