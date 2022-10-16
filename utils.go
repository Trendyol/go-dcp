package main

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"reflect"
	"strconv"
)

func GetCheckpointId(vbId int, groupName string) string {
	key := Prefix + groupName + ":checkpoint:" + strconv.Itoa(vbId)
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
