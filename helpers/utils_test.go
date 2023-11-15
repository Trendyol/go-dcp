package helpers

import (
	"testing"
)

const key = "test"

func TestIsMetadata_ReturnsTrue_WhenStructHasKeyPrefix(t *testing.T) {
	type ts struct {
		Key []byte
	}

	testData := ts{
		Key: []byte(Prefix + key),
	}

	if !IsMetadata(testData) {
		t.Errorf("IsMetadata() = %v, want %v", IsMetadata(testData), true)
	}
}

func TestIsMetadata_ReturnsTrue_WhenKeyHasTxnPrefix(t *testing.T) {
	type ts struct {
		Key []byte
	}

	testData := ts{
		Key: []byte(TxnPrefix + key),
	}

	if !IsMetadata(testData) {
		t.Errorf("IsMetadata() = %v, want %v", IsMetadata(testData), true)
	}
}

func TestIsMetadata_ReturnsFalse_WhenKeyHasNoPrefix(t *testing.T) {
	type ts struct {
		Key []byte
	}

	testData := ts{
		Key: []byte(key),
	}

	if IsMetadata(testData) {
		t.Errorf("IsMetadata() = %v, want %v", IsMetadata(testData), false)
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
