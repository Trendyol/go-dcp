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
	Path    string `yaml:"path" default:"/metrics"`
	Enabled bool   `yaml:"enabled" default:"true"`
}

type ConfigLeaderElection struct {
	Config  map[string]string `yaml:"config"`
	Type    string            `yaml:"type"`
	RPC     ConfigRPC         `yaml:"rpc"`
	Enabled bool              `yaml:"enabled" default:"false"`
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
	LeaderElection  ConfigLeaderElection `yaml:"leaderElector"`
	Metric          ConfigMetric         `yaml:"metric"`
	BucketName      string               `yaml:"bucketName"`
	ScopeName       string               `yaml:"scopeName" default:"_default"`
	CollectionNames []string             `yaml:"collectionNames"`
	MetadataBucket  string               `yaml:"metadataBucket"`
	Password        string               `yaml:"password"`
	Username        string               `yaml:"username"`
	Logging         ConfigLogging        `yaml:"logging"`
	Hosts           []string             `yaml:"hosts"`
	Checkpoint      ConfigCheckpoint     `yaml:"checkpoint"`
	Dcp             ConfigDCP            `yaml:"dcp"`
	API             ConfigAPI            `yaml:"api"`
}

func (c *Config) IsCollectionModeEnabled() bool {
	return !(c.ScopeName == DefaultScopeName && len(c.CollectionNames) == 1 && c.CollectionNames[0] == DefaultCollectionName)
}

func Options(opts *config.Options) {
	opts.ParseTime = true
	opts.Readonly = true
	opts.EnableCache = true
	opts.ParseDefault = true
}

func applyUnhandledDefaults(_config *Config) {
	if _config.Checkpoint.Interval == 0 {
		_config.Checkpoint.Interval = 10 * time.Second
	}

	if _config.MetadataBucket == "" {
		_config.MetadataBucket = _config.BucketName
	}

	if _config.CollectionNames == nil {
		_config.CollectionNames = []string{DefaultCollectionName}
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
