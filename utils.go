package godcpclient

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"reflect"
	"strconv"
)

func GetCheckpointId(vbId uint16, groupName string, userAgent string) string {
	// _connector:cbgo:groupName:stdout-listener:checkpoint:vbId#crcVbId
	key := Prefix + groupName + ":" + userAgent + ":checkpoint:" + strconv.Itoa(int(vbId))
	crc := crc32.Checksum([]byte(fmt.Sprintf("%x", vbId)), crc32.IEEETable)
	return fmt.Sprintf("%v#%08x", key, crc)
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
