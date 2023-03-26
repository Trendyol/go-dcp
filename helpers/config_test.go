package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	configPath, configFileClean, err := CreateConfigFile()
	if err != nil {
		t.Error(err)
	}
	defer configFileClean()

	config := NewConfig(Name, configPath)

	assert.Equal(t, 1, len(config.Hosts))
	assert.Contains(t, config.Hosts, "localhost:8091")
	assert.Equal(t, "sample", config.BucketName)
	assert.Equal(t, "Administrator", config.Username)
	assert.Equal(t, "password", config.Password)
}
