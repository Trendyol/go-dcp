package helpers

import (
	"bytes"
	"reflect"
	"strconv"
	"strings"
	"time"
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

func Retry(f func() error, attempts int, sleep time.Duration) (err error) {
	for i := 0; i < attempts; i++ {
		if i > 0 {
			time.Sleep(sleep)
		}

		err = f()
		if err == nil {
			return nil
		}
	}

	return err
}

const OneMbInBytes = 1_048_576

func MBToBytes(str string) uint {
	str = strings.ToUpper(str)

	s := strings.Split(str, "MB")

	s[0] = strings.ReplaceAll(s[0], ",", ".")

	mb, _ := strconv.ParseFloat(s[0], 64)
	return uint(mb * OneMbInBytes)
}
