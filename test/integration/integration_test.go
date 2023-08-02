package integration

import (
	"context"
	"github.com/Trendyol/go-dcp"
	"github.com/Trendyol/go-dcp/models"
	"sync"
	"testing"
	"time"
)

var mutationCount = 0
var deletionCount = 0
var expirationCount = 0

func listener(ctx *models.ListenerContext) {
	switch ctx.Event.(type) {
	case models.DcpMutation:
		mutationCount += 1
	case models.DcpDeletion:
		deletionCount += 1
	case models.DcpExpiration:
		expirationCount += 1
	}
	ctx.Ack()
}

func TestCouchbase(t *testing.T) {
	newDcp, err := dcp.NewDcp("config.yml", listener)
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		newDcp.Start()
	}()

	go func() {
		time.Sleep(5 * time.Second)
		ctx, _ := context.WithTimeout(context.Background(), 20*time.Second)
		for {
			select {
			case <-ctx.Done():
				t.Fatalf("could not process events in 20 seconds")
			default:
				if mutationCount == 31591 {
					newDcp.Close()
					return
				}
				time.Sleep(1 * time.Second)
			}
		}
	}()

	wg.Wait()
	t.Log("done done done")
}
