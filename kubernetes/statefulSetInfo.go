package kubernetes

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

type StatefulSetInfo struct {
	name       string
	podOrdinal int
}

func NewStatefulSetInfoFromHostname() (*StatefulSetInfo, error) {
	hostname, err := os.Hostname()

	if err != nil {
		return nil, err
	}

	separatorIndex := strings.LastIndex(hostname, "-")

	if separatorIndex == -1 {
		return nil, errors.New("hostname is not in statefulSet format")
	}

	name := hostname[:separatorIndex]
	podOrdinal, err := strconv.Atoi(hostname[separatorIndex+1:])

	if err != nil {
		return nil, err
	}

	return &StatefulSetInfo{
		name:       name,
		podOrdinal: podOrdinal,
	}, nil
}
