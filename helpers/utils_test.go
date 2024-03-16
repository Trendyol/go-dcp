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

func TestChunkSlice(t *testing.T) {
	size := 1009
	slice := make([]int, size)
	for i := 0; i < size; i++ {
		slice[i] = 0
	}

	chunks := ChunkSlice[int](slice, 6)

	if len(chunks) != 6 {
		t.Errorf("ChunkSliceWithSize failed")
	}

	for idx, chunk := range chunks {
		if idx+1 == 1 && len(chunk) == 169 {
			continue
		}

		if len(chunk) == 168 {
			continue
		}

		t.Errorf("ChunkSliceWithSize failed")
	}
}

func TestChunkSliceWithSize(t *testing.T) {
	size := 1001
	slice := make([]int, size)
	for i := 0; i < size; i++ {
		slice[i] = 0
	}

	chunks := ChunkSliceWithSize[int](slice, 5)

	if len(chunks) != 201 {
		t.Errorf("ChunkSliceWithSize failed")
	}

	for idx, chunk := range chunks {
		if len(chunk) == 5 {
			continue
		}

		if idx+1 == 201 && len(chunk) == 1 {
			continue
		}

		t.Errorf("ChunkSliceWithSize failed")
	}

	chunks = ChunkSliceWithSize[int]([]int{0, 0, 0, 0}, 5)

	if len(chunks) != 1 {
		t.Errorf("ChunkSliceWithSize failed")
	}

	for _, chunk := range chunks {
		if len(chunk) == 4 {
			continue
		}

		t.Errorf("ChunkSliceWithSize failed")
	}

	chunks = ChunkSliceWithSize[int]([]int{0, 0, 0, 0, 0, 0, 0, 0, 0}, 5)

	if len(chunks) != 2 {
		t.Errorf("ChunkSliceWithSize failed")
	}

	for idx, chunk := range chunks {
		if len(chunk) == 5 {
			continue
		}

		if idx+1 == 2 && len(chunk) == 4 {
			continue
		}

		t.Errorf("ChunkSliceWithSize failed")
	}

	chunks = ChunkSliceWithSize[int]([]int{}, 5)

	if len(chunks) != 0 {
		t.Errorf("ChunkSliceWithSize failed")
	}
}
