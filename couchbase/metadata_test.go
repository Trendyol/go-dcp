package couchbase

import (
	"testing"
)

func TestGetCheckpointID(t *testing.T) {
	// Define test cases with input and expected output
	testCases := []struct {
		vbID       uint16
		groupName  string
		expectedID string
	}{
		{0, "group:1", "_connector:cbgo:group%3A1:checkpoint:0"},
		{42, "group.2", "_connector:cbgo:group%2E2:checkpoint:42"},
	}

	for _, tc := range testCases {
		t.Run(tc.groupName, func(t *testing.T) {
			result := string(getCheckpointID(tc.vbID, tc.groupName))

			if result != tc.expectedID {
				t.Errorf("getCheckpointID(%d, %s) = got %s; want %s", tc.vbID, tc.groupName, result, tc.expectedID)
			}
		})
	}
}
