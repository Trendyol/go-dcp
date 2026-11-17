package integration

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp"
	"github.com/Trendyol/go-dcp/models"
)

var (
	mutationCount   atomic.Uint64
	deletionCount   atomic.Uint64
	expirationCount atomic.Uint64
)

func listener(ctx *models.ListenerContext) {
	switch ctx.Event.(type) {
	case models.DcpMutation:
		mutationCount.Add(1)
	case models.DcpDeletion:
		deletionCount.Add(1)
	case models.DcpExpiration:
		expirationCount.Add(1)
	}
	ctx.Ack()
}

func TestCouchbase(t *testing.T) {
	newDcp, err := dcp.NewDcp("config.yml", listener)
	if err != nil {
		log.Fatalf("couldn't create dcp err: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		newDcp.Start()
	}()

	go func() {
		ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
		for {
			select {
			case <-ctx.Done():
				t.Fatalf("could not process events in 30 seconds")
			default:
				if mutationCount.Load() == 31591 {
					newDcp.Close()
					return
				}
				time.Sleep(time.Second)
			}
		}
	}()

	wg.Wait()
}
