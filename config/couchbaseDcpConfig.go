package config

import (
	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yamlv3"
)

type CouchbaseDCPConfig struct {
	Hosts      []string `mapstructure:"hosts"`
	Username   string   `mapstructure:"username"`
	Password   string   `mapstructure:"password"`
	BucketName string   `mapstructure:"bucketName"`
	Dcp        struct {
		MetadataBucket string `mapstructure:"metadataBucket"`
		Compression    bool   `mapstructure:"compression"`
		Group          struct {
			Name string `mapstructure:"name"`
		} `mapstructure:"group"`
	} `mapstructure:"dcp"`
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

	return couchbaseDCPConfig
}
