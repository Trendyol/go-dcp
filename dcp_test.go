package godcpclient

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/config"

	"github.com/Trendyol/go-dcp-client/couchbase"
	"github.com/Trendyol/go-dcp-client/models"

	"github.com/couchbase/gocbcore/v10"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var c = &config.Dcp{
	Hosts:      []string{"localhost:8091"},
	Username:   "user",
	Password:   "password",
	BucketName: "dcp-test",
	Dcp: config.ExternalDcp{
		Group: config.DCPGroup{
			Name: "groupName",
			Membership: config.DCPGroupMembership{
				RebalanceDelay: 3 * time.Second,
			},
		},
	},
	Debug: true,
}

func setupContainer(ctx context.Context) (testcontainers.Container, error) {
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "docker.io/trendyoltech/couchbase-testcontainer:6.5.1",
			ExposedPorts: []string{"8091:8091/tcp", "8093:8093/tcp", "11210:11210/tcp"},
			WaitingFor:   wait.ForLog("/entrypoint.sh couchbase-server").WithStartupTimeout(20 * time.Second),
			Env: map[string]string{
				"USERNAME":       c.Username,
				"PASSWORD":       c.Password,
				"BUCKET_NAME":    c.BucketName,
				"BUCKET_RAMSIZE": "1024",
			},
		},
		Started: true,
	})
}

func insertDataToContainer(b *testing.B, iteration int, chunkSize int, bulkSize int) {
	logger.Log.Printf("mock data stream started with iteration=%v", iteration)

	client := couchbase.NewClient(c)

	err := client.Connect()
	if err != nil {
		b.Error(err)
	}

	var iter int

	for iteration > iter {
		for chunk := 0; chunk < chunkSize; chunk++ {
			wg := &sync.WaitGroup{}
			wg.Add(bulkSize)

			for id := 0; id < 2048; id++ {
				go func(id int, chunk int) {
					ch := make(chan error)

					opm := couchbase.NewAsyncOp(context.Background())

					op, err := client.GetAgent().Set(gocbcore.SetOptions{
						Key:   []byte(fmt.Sprintf("%v_%v_%v", iter, chunk, id)),
						Value: []byte(fmt.Sprintf("%v_%v_%v", iter, chunk, id)),
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
				}(id, chunk)
			}

			wg.Wait()
		}

		iter++
	}

	client.Close()

	logger.Log.Printf("mock data stream finished with totalSize=%v", iteration)
}

//nolint:funlen
func BenchmarkDcp(b *testing.B) {
	chunkSize := 8
	bulkSize := 2048
	iteration := 72
	mockDataSize := iteration * bulkSize * chunkSize
	totalNotify := 10
	notifySize := mockDataSize / totalNotify

	ctx := context.Background()

	container, err := setupContainer(ctx)
	if err != nil {
		b.Error(err)
	}

	counter := 0
	finish := make(chan struct{}, 1)

	dcp, err := NewDcp(c, func(ctx *models.ListenerContext) {
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
		insertDataToContainer(b, iteration, chunkSize, bulkSize)
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
}
