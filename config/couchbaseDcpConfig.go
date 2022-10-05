package config

import (
	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yamlv3"
	"strings"
)

type CouchbaseDCPConfigDCPGroup struct {
	Name string `mapstructure:"name"`
}

type CouchbaseDCPConfigDCP struct {
	MetadataBucket             string                     `mapstructure:"metadataBucket"`
	Compression                bool                       `mapstructure:"compression"`
	Group                      CouchbaseDCPConfigDCPGroup `mapstructure:"group"`
	FlowControlBuffer          int                        `mapstructure:"flowControlBuffer"`
	PersistencePollingInterval int                        `mapstructure:"persistencePollingInterval"`
}

type CouchbaseDCPConfig struct {
	Hosts      []string              `mapstructure:"hosts"`
	Username   string                `mapstructure:"username"`
	Password   string                `mapstructure:"password"`
	BucketName string                `mapstructure:"bucketName"`
	Dcp        CouchbaseDCPConfigDCP `mapstructure:"dcp"`
}

func LoadConfig(filePath string) CouchbaseDCPConfig {
	config.WithOptions(config.ParseEnv)
	config.AddDriver(yamlv3.Driver)
	err := config.LoadFiles(filePath)
	if err != nil {
		panic(err)
	}
	couchbaseDCPConfig := CouchbaseDCPConfig{}
	err = config.BindStruct("couchbase", &couchbaseDCPConfig)

	if err != nil {
		panic(err)
	}

	hosts := config.String("couchbase.hosts")

	var hostItems []string
	for _, element := range strings.Split(hosts, ",") {
		hostItems = append(hostItems, element)
	}
	couchbaseDCPConfig.Hosts = hostItems

	return couchbaseDCPConfig
}
