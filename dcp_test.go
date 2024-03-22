package dcp

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp/logger"

	"github.com/Trendyol/go-dcp/config"

	"github.com/Trendyol/go-dcp/couchbase"
	"github.com/couchbase/gocbcore/v10"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func panicVersion(version string) {
	panic(fmt.Sprintf("invalid version: %v", version))
}

func parseVersion(version string) (int, int, int) {
	parse := strings.Split(version, ".")
	if len(parse) < 3 {
		panicVersion(version)
	}

	major, err := strconv.Atoi(parse[0])
	if err != nil {
		panicVersion(version)
	}

	minor, err := strconv.Atoi(parse[1])
	if err != nil {
		panicVersion(version)
	}

	patch, err := strconv.Atoi(parse[2])
	if err != nil {
		panicVersion(version)
	}

	return major, minor, patch
}

func isVersion5xx(version string) bool {
	major, _, _ := parseVersion(version)
	return major == 5
}

func getConfig() *config.Dcp {
	return &config.Dcp{
		Hosts:      []string{"localhost:8091"},
		Username:   "user",
		Password:   "123456",
		BucketName: "dcp-test",
		Dcp: config.ExternalDcp{
			Group: config.DCPGroup{
				Name: "groupName",
				Membership: config.DCPGroupMembership{
					RebalanceDelay: 3 * time.Second,
				},
			},
		},
	}
}

func setupContainer(c *config.Dcp, ctx context.Context, version string) (testcontainers.Container, error) {
	var entrypoint string
	if isVersion5xx(version) {
		entrypoint = "scripts/entrypoint_5.sh"
	} else {
		entrypoint = "scripts/entrypoint.sh"
	}

	req := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("couchbase/server:%v", version),
		ExposedPorts: []string{"8091:8091/tcp", "8093:8093/tcp", "11210:11210/tcp"},
		WaitingFor:   wait.ForLog("/entrypoint.sh couchbase-server").WithStartupTimeout(30 * time.Second),
		Env: map[string]string{
			"USERNAME":                  c.Username,
			"PASSWORD":                  c.Password,
			"BUCKET_NAME":               c.BucketName,
			"BUCKET_TYPE":               "couchbase",
			"BUCKET_RAMSIZE":            "1024",
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
				HostFilePath:      entrypoint,
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

func insertDataToContainer(c *config.Dcp, t *testing.T, iteration int, chunkSize int, bulkSize int) {
	logger.Log.Info("mock data stream started with iteration=%v", iteration)

	client := couchbase.NewClient(c)

	err := client.Connect()
	if err != nil {
		t.Fatal(err)
	}

	var iter int

	for iteration > iter {
		for chunk := 0; chunk < chunkSize; chunk++ {
			wg := &sync.WaitGroup{}
			wg.Add(bulkSize)

			for id := 0; id < bulkSize; id++ {
				go func(id int, chunk int) {
					ch := make(chan error, 1)

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
						t.Error(err)
					}

					err = <-ch
					if err != nil {
						t.Error(err)
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
func test(t *testing.T, version string) {
	chunkSize := 4
	bulkSize := 1024
	iteration := 512
	mockDataSize := iteration * bulkSize * chunkSize
	totalNotify := 10
	notifySize := mockDataSize / totalNotify

	c := getConfig()
	c.ApplyDefaults()

	ctx := context.Background()

	container, err := setupContainer(c, ctx, version)
	if err != nil {
		t.Fatal(err)
	}

	counter := 0
	finish := make(chan struct{}, 1)

	dcp, err := NewDcp(c, func(ctx *models.ListenerContext) {
		if _, ok := ctx.Event.(models.DcpMutation); ok {
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
		t.Fatal(err)
	}

	go func() {
		<-dcp.WaitUntilReady()
		insertDataToContainer(c, t, iteration, chunkSize, bulkSize)
	}()

	go func() {
		<-finish
		dcp.Close()
	}()

	dcp.Start()

	err = container.Terminate(ctx)
	if err != nil {
		t.Fatal(err)
	}

	logger.Log.Info("mock data stream finished with totalSize=%v", counter)
}

func TestDcp(t *testing.T) {
	version := os.Getenv("CB_VERSION")

	if version == "" {
		t.Skip("Skipping test")
	}

	t.Run(version, func(t *testing.T) {
		test(t, version)
	})
}
