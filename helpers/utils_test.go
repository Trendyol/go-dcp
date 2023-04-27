package helpers

import (
	"testing"
)

func TestIsMetadata_ReturnsTrue_WhenStructHasKeyPrefix(t *testing.T) {
	type ts struct {
		Key []byte
	}

	testData := ts{
		Key: []byte(Prefix + "test"),
	}

	if !IsMetadata(testData) {
		t.Errorf("IsMetadata() = %v, want %v", IsMetadata(testData), true)
	}
}

func TestIsMetadata_ReturnsFalse_WhenStructHasNoKeyPrefix(t *testing.T) {
	type ts struct {
		X []byte
	}

	testData := ts{
		X: []byte(Prefix + "test"),
	}

	if IsMetadata(testData) {
		t.Errorf("IsMetadata() = %v, want %v", IsMetadata(testData), false)
	}
}
