package couchbase

import (
	"bytes"
	"testing"
)

func TestGetCheckpointID(t *testing.T) {
	expected := []byte("_connector:cbgo:group1:checkpoint:1")
	actual := getCheckpointID(uint16(1), "group1")
	if !bytes.Equal(actual, expected) {
		t.Errorf("Unexpected result. Expected: %s, Got: %s", expected, actual)
	}
}
