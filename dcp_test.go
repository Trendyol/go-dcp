package dcp

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp/logger"

	"github.com/Trendyol/go-dcp/config"

	"github.com/Trendyol/go-dcp/couchbase"
	"github.com/Trendyol/go-dcp/models"

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
	req := testcontainers.ContainerRequest{
		Image:        "couchbase:6.5.1",
		ExposedPorts: []string{"8091:8091/tcp", "8093:8093/tcp", "11210:11210/tcp"},
		WaitingFor:   wait.ForLog("/entrypoint.sh couchbase-server").WithStartupTimeout(20 * time.Second),
		Env: map[string]string{
			"USERNAME":                  c.Username,
			"PASSWORD":                  c.Password,
			"BUCKET_NAME":               c.BucketName,
			"BUCKET_TYPE":               "couchbase",
			"BUCKET_RAMSIZE":            "1024",
			"SERVICES":                  "data,index,query,fts,analytics,eventing",
			"CLUSTER_RAMSIZE":           "1024",
			"CLUSTER_INDEX_RAMSIZE":     "512",
			"CLUSTER_EVENTING_RAMSIZE":  "256",
			"CLUSTER_FTS_RAMSIZE":       "256",
			"CLUSTER_ANALYTICS_RAMSIZE": "1024",
			"INDEX_STORAGE_SETTING":     "memopt",
			"REST_PORT":                 "8091",
			"CAPI_PORT":                 "8092",
			"QUERY_PORT":                "8093",
			"FTS_PORT":                  "8094",
			"MEMCACHED_SSL_PORT":        "11207",
			"MEMCACHED_PORT":            "11210",
			"SSL_REST_PORT":             "18091",
		},
		Entrypoint: []string{
			"/config-entrypoint.sh",
		},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      "scripts/entrypoint.sh",
				ContainerFilePath: "/config-entrypoint.sh",
				FileMode:          600,
			},
		},
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func insertDataToContainer(b *testing.B, iteration int, chunkSize int, bulkSize int) {
	logger.Log.Info("mock data stream started with iteration=%v", iteration)

	client := couchbase.NewClient(c)

	err := client.Connect()
	if err != nil {
		b.Fatal(err)
	}

	var iter int

	for iteration > iter {
		for chunk := 0; chunk < chunkSize; chunk++ {
			wg := &sync.WaitGroup{}
			wg.Add(bulkSize)

			for id := 0; id < bulkSize; id++ {
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

	logger.Log.Info("mock data stream finished with totalSize=%v", iteration)
}

//nolint:funlen
func BenchmarkDcp(b *testing.B) {
	chunkSize := 4
	bulkSize := 1024
	iteration := 24
	mockDataSize := iteration * bulkSize * chunkSize
	totalNotify := 10
	notifySize := mockDataSize / totalNotify

	ctx := context.Background()

	container, err := setupContainer(ctx)
	if err != nil {
		b.Fatal(err)
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
				logger.Log.Info("%v/%v processed", counter/notifySize, totalNotify)
			}

			if counter == mockDataSize {
				finish <- struct{}{}
			}
		}
	})
	if err != nil {
		b.Fatal(err)
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
		b.Fatal(err)
	}
}
