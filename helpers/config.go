package helpers

import (
	"time"

	"github.com/Trendyol/go-dcp-client/logger"
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
	Port int `yaml:"port" default:"8080"`
}

type ConfigMetric struct {
	Enabled bool   `yaml:"enabled" default:"true"`
	Path    string `yaml:"path" default:"/metrics"`
}

type ConfigLeaderElection struct {
	Enabled bool              `yaml:"enabled" default:"false"`
	Type    string            `yaml:"type"`
	Config  map[string]string `yaml:"config"`
	RPC     ConfigRPC         `yaml:"rpc"`
}

type ConfigRPC struct {
	Port int `yaml:"port" default:"8081"`
}

type ConfigLogging struct {
	Level string `yaml:"level" default:"info"`
}

type ConfigCheckpoint struct {
	Type     string        `yaml:"type" default:"auto"`
	Interval time.Duration `yaml:"interval"`
}

type Config struct {
	Hosts          []string             `yaml:"hosts"`
	Username       string               `yaml:"username"`
	Password       string               `yaml:"password"`
	BucketName     string               `yaml:"bucketName"`
	ScopeName      string               `yaml:"scopeName" default:"_default"`
	CollectionName string               `yaml:"collectionName" default:"_default"`
	MetadataBucket string               `yaml:"metadataBucket"`
	Dcp            ConfigDCP            `yaml:"dcp"`
	API            ConfigAPI            `yaml:"api"`
	Metric         ConfigMetric         `yaml:"metric"`
	LeaderElection ConfigLeaderElection `yaml:"leaderElector"`
	Logging        ConfigLogging        `yaml:"logging"`
	Checkpoint     ConfigCheckpoint     `yaml:"checkpoint"`
}

func Options(opts *config.Options) {
	opts.ParseTime = true
	opts.Readonly = true
	opts.EnableCache = true
	opts.ParseDefault = true
}

func applyUnhandledDefaults(_config *Config) {
	if _config.Checkpoint.Interval == 0 {
		_config.Checkpoint.Interval = 5 * time.Second
	}

	if _config.MetadataBucket == "" {
		_config.MetadataBucket = _config.BucketName
	}
}

func NewConfig(name string, filePath string) Config {
	conf := config.New(name).WithOptions(Options).WithDriver(yamlv3.Driver)

	err := conf.LoadFiles(filePath)
	if err != nil {
		logger.Panic(err, "cannot load config file")
	}

	_config := Config{}
	err = conf.Decode(&_config)

	if err != nil {
		logger.Panic(err, "cannot decode config file")
	}

	logger.Debug("config loaded from file: %v", filePath)

	applyUnhandledDefaults(&_config)

	return _config
}
