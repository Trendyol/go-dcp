package helpers

import (
	"bytes"
	"os"
	"reflect"
	"runtime"

	"github.com/Trendyol/go-dcp-client/logger"
)

func IsMetadata(data interface{}) bool {
	value := reflect.ValueOf(data).FieldByName("Key")
	if !value.IsValid() {
		return false
	}

	return bytes.HasPrefix(value.Bytes(), []byte(Prefix))
}

func ChunkSlice[T any](slice []T, chunks int) [][]T {
	maxChunkSize := ((len(slice) - 1) / chunks) + 1
	numFullChunks := chunks - (maxChunkSize*chunks - len(slice))

	result := make([][]T, chunks)

	startIndex := 0

	for i := 0; i < chunks; i++ {
		endIndex := startIndex + maxChunkSize

		if i >= numFullChunks {
			endIndex--
		}

		result[i] = slice[startIndex:endIndex]

		startIndex = endIndex
	}

	return result
}

func CreateConfigFile(content string) (*os.File, error) {
	tmpFile, err := os.CreateTemp("", "*.yml")
	if err != nil {
		return nil, err
	}

	if _, err = tmpFile.WriteString(content); err != nil {
		return nil, err
	}

	return tmpFile, nil
}

func byteToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func LogMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	logger.Log.Printf(
		"memory usage: alloc = %vmb, totalAlloc = %vmb, sys = %vmb",
		byteToMb(m.Alloc),
		byteToMb(m.TotalAlloc),
		byteToMb(m.Sys),
	)
}
