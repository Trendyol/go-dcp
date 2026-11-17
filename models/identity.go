package models

import (
	"github.com/Trendyol/go-dcp/logger"
	"github.com/bytedance/sonic"
)

type Identity struct {
	IP              string
	Name            string
	ClusterJoinTime int64
}

func (k *Identity) String() string {
	str, err := sonic.Marshal(k)
	if err != nil {
		logger.Log.Error("error while marshalling identity, err: %v", err)
		panic(err)
	}

	return string(str)
}

func (k *Identity) Equal(other *Identity) bool {
	return k.IP == other.IP && k.Name == other.Name
}

func NewIdentityFromStr(str string) *Identity {
	var identity Identity

	err := sonic.Unmarshal([]byte(str), &identity)
	if err != nil {
		logger.Log.Error("error while unmarshalling identity, err: %v", err)
		panic(err)
	}

	return &identity
}
