package integration

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	godcp "github.com/Trendyol/go-dcp"
	"github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/couchbase"
	"github.com/Trendyol/go-dcp/models"
	"github.com/couchbase/gocbcore/v10"
)

func TestDcpFiniteMode(t *testing.T) {
	if os.Getenv("DCP_FINITE_LOCAL_ONLY") != "yes" {
		t.Skip("requires an isolated local Couchbase network")
	}
	password := os.Getenv("DCP_FINITE_PASSWORD")
	if password == "" {
		t.Fatal("local Couchbase password is required")
	}
	cfg := &config.Dcp{
		Hosts: []string{"dcp-poc-cb:8091"}, Username: "pocadmin", Password: password,
		BucketName: "poc-source", API: config.API{Disabled: true},
		Logging:            config.Logging{Level: "error"},
		Metadata:           config.Metadata{Type: "couchbase", Config: map[string]string{"bucket": "poc-meta"}},
		Checkpoint:         config.Checkpoint{Type: "manual", AutoReset: "earliest"},
		RollbackMitigation: config.RollbackMitigation{Disabled: true},
		Dcp: config.ExternalDcp{Mode: config.DcpModeFinite, Group: config.DCPGroup{
			Name: "finite-mode-test", Membership: config.DCPGroupMembership{Type: "static"},
		}},
	}
	cfg.ApplyDefaults()
	writer := couchbase.NewClient(cfg)
	if err := writer.Connect(); err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	const documents = 256
	for i := range documents {
		result := make(chan error, 1)
		_, err := writer.GetAgent().Set(gocbcore.SetOptions{
			Key: []byte(fmt.Sprintf("finite-%d", i)), Value: []byte(`{"test":"finite"}`),
			Deadline: time.Now().Add(5 * time.Second),
		}, func(_ *gocbcore.StoreResult, err error) { result <- err })
		if err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("local write timed out")
		}
	}
	var mu sync.Mutex
	keys := make(map[string]int)
	consumer, err := godcp.NewDcp(cfg, func(ctx *models.ListenerContext) {
		if event, ok := ctx.Event.(models.DcpMutation); ok {
			mu.Lock()
			keys[string(event.Key)]++
			mu.Unlock()
		}
		ctx.Ack()
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { consumer.Start(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("finite Start did not return without an explicit Close")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != documents {
		t.Fatalf("consumed %d distinct documents, want %d", len(keys), documents)
	}
	for i := range documents {
		key := fmt.Sprintf("finite-%d", i)
		if keys[key] != 1 {
			t.Fatalf("document %s consumed %d times, want 1", key, keys[key])
		}
	}
	t.Logf("consumed all %d documents; Start returned without Close", documents)
}
