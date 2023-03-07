package godcpclient

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	mathRand "math/rand"

	gDcp "github.com/Trendyol/go-dcp-client/dcp"
	"github.com/Trendyol/go-dcp-client/models"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/couchbase/gocbcore/v10"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func createConfigFile() (string, func(), error) {
	configStr := `hosts:
  - localhost:8091
username: Administrator
password: password
bucketName: sample
scopeName: _default
collectionNames:
  - _default
metadataBucket: sample
checkpoint:
  type: manual
logging:
  level: debug
dcp:
  group:
    name: groupName
    membership:
      type: static
      memberNumber: 1
      totalMembers: 1`

	tmpFile, err := os.CreateTemp("", "*.yml")
	if err != nil {
		return "", nil, err
	}

	if _, err = tmpFile.WriteString(configStr); err != nil {
		return "", nil, err
	}

	return tmpFile.Name(), func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}, nil
}

func setupContainer(b *testing.B, config helpers.Config) func() {
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

func insertDataToContainer(b *testing.B, mockDataSize int64, config helpers.Config) {
	client := gDcp.NewClient(config)

	_ = client.Connect()
	defer client.Close()

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
			wg := sync.WaitGroup{}
			wg.Add(len(chunk))

			for _, id := range chunk {
				go func(i int) {
					ch := make(chan error)

					opm := helpers.NewAsyncOp(context.Background())

					op, err := client.GetAgent().Set(gocbcore.SetOptions{
						Key:   []byte(fmt.Sprintf("k%v", i)),
						Value: []byte(fmt.Sprintf("v%v", i)),
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

	log.Printf("Inserted %v items", mockDataSize)
}

func BenchmarkDcp(benchmark *testing.B) {
	b, err := rand.Int(rand.Reader, big.NewInt(360000)) //nolint:gosec
	if err != nil {
		benchmark.Error(err)
	}

	mockDataSize := b.Int64() + 120000
	saveTarget := mathRand.Intn(int(mockDataSize))

	configPath, configFileClean, err := createConfigFile()
	if err != nil {
		benchmark.Error(err)
	}
	defer configFileClean()

	config := helpers.NewConfig(fmt.Sprintf("%v_data_insert", helpers.Name), configPath)

	containerShutdown := setupContainer(benchmark, config)
	defer containerShutdown()

	insertDataToContainer(benchmark, mockDataSize, config)

	var dcp Dcp

	var lock sync.Mutex
	var counter int

	dcp, err = NewDcp(configPath, func(ctx *models.ListenerContext) {
		if _, ok := ctx.Event.(models.DcpMutation); ok {
			ctx.Ack()

			lock.Lock()
			counter++

			if counter == saveTarget {
				ctx.Commit()
			}

			if counter == int(mockDataSize) {
				dcp.Close()
			}

			lock.Unlock()
		}
	})

	if err != nil {
		benchmark.Error(err)
	}

	dcp.Start()
}
