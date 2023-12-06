package integration

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp"
	"github.com/Trendyol/go-dcp/models"
)

var (
	mutationCount   = 0
	deletionCount   = 0
	expirationCount = 0
)

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
	time.Sleep(10 * time.Second)
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
				time.Sleep(time.Second)
			}
		}
	}()

	time.Sleep(time.Second)

	wg.Wait()
}
