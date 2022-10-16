package main

import (
	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yamlv3"
	"strings"
)

type ConfigDCPGroupMembership struct {
	MemberNumber int `mapstructure:"memberNumber"`
	TotalMembers int `mapstructure:"totalMembers"`
}

type ConfigDCPGroup struct {
	Name       string                   `mapstructure:"name"`
	Membership ConfigDCPGroupMembership `mapstructure:"membership"`
}

type ConfigDCP struct {
	FlowControlBuffer int            `mapstructure:"flowControlBuffer"`
	Group             ConfigDCPGroup `mapstructure:"group"`
}

type Config struct {
	Hosts          []string  `mapstructure:"hosts"`
	Username       string    `mapstructure:"username"`
	Password       string    `mapstructure:"password"`
	BucketName     string    `mapstructure:"bucketName"`
	UserAgent      string    `mapstructure:"userAgent"`
	Compression    bool      `mapstructure:"compression"`
	MetadataBucket string    `mapstructure:"metadataBucket"`
	Dcp            ConfigDCP `mapstructure:"dcp"`
}

func NewConfig(filePath string) Config {
	config.WithOptions(config.ParseEnv)
	config.AddDriver(yamlv3.Driver)

	err := config.LoadFiles(filePath)

	if err != nil {
		panic(err)
	}

	_config := Config{}
	err = config.BindStruct("couchbase", &_config)

	if err != nil {
		panic(err)
	}

	hosts := config.String("couchbase.hosts")
	_config.Hosts = strings.Split(hosts, ",")

	return _config
}
