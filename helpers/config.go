package helpers

import (
	"log"

	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yamlv3"
)

type ConfigDCPGroupMembership struct {
	Type         string `yaml:"type"`
	MemberNumber int    `yaml:"memberNumber"`
	TotalMembers int    `yaml:"totalMembers"`
}

type ConfigDCPGroup struct {
	Name       string                   `yaml:"name"`
	Membership ConfigDCPGroupMembership `yaml:"membership"`
}

type ConfigDCP struct {
	Group ConfigDCPGroup `yaml:"group"`
}

type ConfigAPI struct {
	Port int `yaml:"port"`
}

type ConfigMetric struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
}

type ConfigLeaderElection struct {
	Enabled bool              `yaml:"enabled"`
	Type    string            `yaml:"type"`
	Config  map[string]string `yaml:"config"`
	RPC     ConfigRPC         `yaml:"rpc"`
}

type ConfigRPC struct {
	Port int `yaml:"port"`
}

type Config struct {
	Hosts          []string             `yaml:"hosts"`
	Username       string               `yaml:"username"`
	Password       string               `yaml:"password"`
	BucketName     string               `yaml:"bucketName"`
	MetadataBucket string               `yaml:"metadataBucket"`
	Dcp            ConfigDCP            `yaml:"dcp"`
	API            ConfigAPI            `yaml:"api"`
	Metric         ConfigMetric         `yaml:"metric"`
	LeaderElection ConfigLeaderElection `yaml:"leaderElector"`
}

func Options(opts *config.Options) {
	opts.ParseTime = true
	opts.Readonly = true
	opts.EnableCache = true
}

func NewConfig(name string, filePath string) Config {
	conf := config.New(name).WithOptions(Options).WithDriver(yamlv3.Driver)

	err := conf.LoadFiles(filePath)
	if err != nil {
		panic(err)
	}

	_config := Config{}
	err = conf.Decode(&_config)

	if err != nil {
		panic(err)
	}

	log.Printf("config loaded from file: %v", filePath)

	return _config
}
