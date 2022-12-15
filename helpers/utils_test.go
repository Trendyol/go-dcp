package helpers

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIsMetadata_ReturnsTrue_WhenStructHasKeyPrefix(t *testing.T) {
	type ts struct {
		Key []byte
	}

	testData := ts{
		Key: []byte(Prefix + "test"),
	}

	assert.True(t, IsMetadata(testData))
}

func TestIsMetadata_ReturnsFalse_WhenStructHasNoKeyPrefix(t *testing.T) {
	type ts struct {
		X []byte
	}

	testData := ts{
		X: []byte(Prefix + "test"),
	}

	assert.False(t, IsMetadata(testData))
}
