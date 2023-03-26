package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var configStr = `hosts:
  - localhost:8091
username: Administrator
password: password
bucketName: sample
scopeName: _default
collectionNames:
  - _default
metadata:
  config:
    bucket: sample
checkpoint:
  type: manual
logging:
  level: debug
dcp:
  listener:
    bufferSize: 1024
  group:
    name: groupName
    membership:
      type: static`

func TestLoadConfig(t *testing.T) {
	configPath, configFileClean, err := CreateConfigFile(configStr)
	if err != nil {
		t.Error(err)
	}
	defer configFileClean()

	config := NewConfig(Name, configPath)

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
	assert.Equal(t, "debug", config.Logging.Level)
	assert.Equal(t, 1024, config.Dcp.Listener.BufferSize)
	assert.Equal(t, "groupName", config.Dcp.Group.Name)
	assert.Equal(t, "static", config.Dcp.Group.Membership.Type)
}
