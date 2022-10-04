package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	config := LoadConfig("test.yml")

	assert.Contains(t, config.Hosts, "10.10.36.120")
	assert.Equal(t, "MyBucket", config.BucketName)
	assert.Equal(t, "username", config.Username)
	assert.Equal(t, "password", config.Password)
	assert.Equal(t, "MyMetaBucket", config.Dcp.MetadataBucket)
}
