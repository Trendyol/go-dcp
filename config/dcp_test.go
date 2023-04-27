package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	config := Dcp{}
	config.ApplyDefaults()

	assert.Equal(t, "_default", config.ScopeName)
	assert.Equal(t, 1, len(config.CollectionNames))
	assert.Contains(t, config.CollectionNames, "_default")
	assert.Equal(t, "auto", config.Checkpoint.Type)
	assert.Equal(t, uint(1), config.Dcp.Listener.BufferSize)
	assert.Equal(t, "couchbase", config.Dcp.Group.Membership.Type)
}

func TestGetCouchbaseMetadata(t *testing.T) {
	dcp := &Dcp{
		Metadata: Metadata{
			Config: map[string]string{
				CouchbaseMetadataBucketConfig: "mybucket",
				CouchbaseMetadataScopeConfig:  "myscope",
			},
		},
		BucketName: "mybucket2",
	}

	bucket, scope, collection, connectionBufferSize, connectionTimeout := dcp.GetCouchbaseMetadata()

	expectedBucket := "mybucket"
	expectedScope := "myscope"
	expectedCollection := DefaultCollectionName
	expectedConnectionBufferSize := uint(20971520)
	expectedConnectionTimeout := 5 * time.Second

	assert.Equal(t, expectedBucket, bucket)
	assert.Equal(t, expectedScope, scope)
	assert.Equal(t, expectedCollection, collection)
	assert.Equal(t, expectedConnectionBufferSize, connectionBufferSize)
	assert.Equal(t, expectedConnectionTimeout, connectionTimeout)
}

func TestDcp_GetFileMetadata(t *testing.T) {
	dcp := &Dcp{
		Metadata: Metadata{
			Config: map[string]string{
				FileMetadataFileNameConfig: "testfile.json",
			},
		},
	}

	metadata := dcp.GetFileMetadata()

	assert.Equal(t, "testfile.json", metadata)
}

func TestApplyDefaultRollbackMitigation(t *testing.T) {
	c := &Dcp{
		RollbackMitigation: RollbackMitigation{},
	}
	c.applyDefaultRollbackMitigation()

	assert.Equal(t, 200*time.Millisecond, c.RollbackMitigation.Interval)
}

func TestDcpApplyDefaultCheckpoint(t *testing.T) {
	c := &Dcp{}
	c.applyDefaultCheckpoint()

	assert.Equal(t, 20*time.Second, c.Checkpoint.Interval)
	assert.Equal(t, 5*time.Second, c.Checkpoint.Timeout)
	assert.Equal(t, "auto", c.Checkpoint.Type)
	assert.Equal(t, "earliest", c.Checkpoint.AutoReset)
}

func TestDcpApplyDefaultHealthCheck(t *testing.T) {
	c := &Dcp{}
	c.applyDefaultHealthCheck()

	assert.Equal(t, 20*time.Second, c.HealthCheck.Interval)
	assert.Equal(t, 5*time.Second, c.HealthCheck.Timeout)
	assert.True(t, !c.HealthCheck.Disabled)
}

func TestDcpApplyDefaultGroupMembership(t *testing.T) {
	c := &Dcp{}
	c.applyDefaultGroupMembership()

	assert.Equal(t, 20*time.Second, c.Dcp.Group.Membership.RebalanceDelay)
	assert.Equal(t, 1, c.Dcp.Group.Membership.TotalMembers)
	assert.Equal(t, 1, c.Dcp.Group.Membership.MemberNumber)
	assert.Equal(t, "couchbase", c.Dcp.Group.Membership.Type)
}

func TestDcpApplyDefaultConnectionTimeout(t *testing.T) {
	c := &Dcp{}
	c.applyDefaultConnectionTimeout()

	assert.Equal(t, 5*time.Second, c.Dcp.ConnectionTimeout)
	assert.Equal(t, 5*time.Second, c.ConnectionTimeout)
}

func TestDcpApplyDefaultCollections(t *testing.T) {
	c := &Dcp{}
	c.applyDefaultCollections()

	assert.Equal(t, []string{DefaultCollectionName}, c.CollectionNames)
}

func TestDcpApplyDefaultScopeName(t *testing.T) {
	c := &Dcp{}
	c.applyDefaultScopeName()

	assert.Equal(t, DefaultScopeName, c.ScopeName)
}

func TestDcpApplyDefaultConnectionBufferSize(t *testing.T) {
	c := &Dcp{}
	c.applyDefaultConnectionBufferSize()

	assert.Equal(t, uint(20971520), c.ConnectionBufferSize)
}

func TestDcpApplyDefaultMetrics(t *testing.T) {
	c := &Dcp{}
	c.applyDefaultMetrics()

	assert.Equal(t, "/metrics", c.Metric.Path)
	assert.Equal(t, 10.0, c.Metric.AverageWindowSec)
}

func TestDcpApplyDefaultAPI(t *testing.T) {
	c := &Dcp{}
	c.applyDefaultAPI()

	assert.True(t, !c.API.Disabled)
	assert.Equal(t, 8080, c.API.Port)
}

func TestDcpApplyDefaultLeaderElection(t *testing.T) {
	c := &Dcp{}
	c.applyDefaultLeaderElection()

	assert.Equal(t, "kubernetes", c.LeaderElection.Type)
	assert.Equal(t, 8081, c.LeaderElection.RPC.Port)
}

func TestDcpApplyDefaultDcp(t *testing.T) {
	c := &Dcp{}
	c.applyDefaultDcp()

	assert.Equal(t, 16777216, c.Dcp.BufferSize)
	assert.Equal(t, uint(20971520), c.Dcp.ConnectionBufferSize)
	assert.Equal(t, uint(1), c.Dcp.Listener.BufferSize)
}

func TestApplyDefaultMetadata(t *testing.T) {
	// Initialize a Dcp instance with no metadata
	c := &Dcp{
		BucketName: "my-bucket",
		Metadata:   Metadata{},
	}

	// Apply default metadata
	c.applyDefaultMetadata()

	// Check if the default metadata values were applied correctly
	assert.Equal(t, "couchbase", c.Metadata.Type)
}
