package membership

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type StatefulSetInfo struct {
	PodOrdinal int
}

func NewStatefulSetInfoFromHostname() (*StatefulSetInfo, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	separatorIndex := strings.LastIndex(hostname, "-")

	if separatorIndex == -1 {
		return nil, fmt.Errorf("hostname is not in statefulSet format")
	}

	podOrdinal, err := strconv.Atoi(hostname[separatorIndex+1:])
	if err != nil {
		return nil, err
	}

	return &StatefulSetInfo{
		PodOrdinal: podOrdinal,
	}, nil
}
