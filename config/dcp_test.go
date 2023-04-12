package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	config := DCP{}
	config.ApplyDefaults()

	assert.Equal(t, 1, len(config.Hosts))
	assert.Contains(t, config.Hosts, "localhost:8091")
	assert.Equal(t, "Administrator", config.Username)
	assert.Equal(t, "password", config.Password)
	assert.Equal(t, "sample", config.BucketName)
	assert.Equal(t, "_default", config.ScopeName)
	assert.Equal(t, 1, len(config.CollectionNames))
	assert.Contains(t, config.CollectionNames, "_default")
	assert.Equal(t, "sample", config.Metadata.Config["bucket"])
	assert.Equal(t, "manual", config.Checkpoint.Type)
	assert.Equal(t, uint(1024), config.Dcp.Listener.BufferSize)
	assert.Equal(t, "groupName", config.Dcp.Group.Name)
	assert.Equal(t, "static", config.Dcp.Group.Membership.Type)
}
