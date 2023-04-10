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
	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/couchbase/gocbcore/v10"
)

var configStr = `hosts:
  - localhost:8091
username: user
password: password
bucketName: dcp-test
checkpoint:
  type: manual
dcp:
  group:
    name: groupName
    membership:
      type: static`

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
	mockDataSize := 6400000
	configPath, configFileClean, err := helpers.CreateConfigFile(configStr)
	if err != nil {
		benchmark.Error(err)
	}
	defer configFileClean()

	config := helpers.NewConfig(fmt.Sprintf("%v_data_insert", helpers.Name), configPath)

	insertDataToContainer(benchmark, mockDataSize, config)
}
