package helpers

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"

	"github.com/google/uuid"
)

func GetCheckpointID(vbID uint16, groupName string) string {
	// _connector:cbgo:groupName:stdout-listener:checkpoint:vbId
	return Prefix + groupName + ":checkpoint:" + strconv.Itoa(int(vbID))
}

func GetDcpStreamName(groupName string) string {
	streamName := fmt.Sprintf("%s_%s", groupName, uuid.New().String())
	return streamName
}

func IsMetadata(data interface{}) bool {
	t := reflect.TypeOf(data)

	_, exist := t.FieldByName("Key")

	if exist {
		key := reflect.ValueOf(data).FieldByName("Key").Bytes()
		return bytes.HasPrefix(key, []byte(Prefix))
	}

	return false
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
