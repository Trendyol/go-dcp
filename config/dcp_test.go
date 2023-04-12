package config

import (
	"testing"

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
