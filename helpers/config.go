package helpers

import (
	"errors"
	"time"

	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yamlv3"
)

type ConfigDCPGroupMembership struct {
	Type           string        `yaml:"type" default:"couchbase"`
	MemberNumber   int           `yaml:"memberNumber" default:"1"`
	TotalMembers   int           `yaml:"totalMembers" default:"1"`
	RebalanceDelay time.Duration `yaml:"rebalanceDelay"`
}

type ConfigDCPGroup struct {
	Name       string                   `yaml:"name"`
	Membership ConfigDCPGroupMembership `yaml:"membership"`
}

type ConfigDCPListener struct {
	BufferSize int `yaml:"bufferSize" default:"1"`
}

type ConfigDCP struct {
	Group                  ConfigDCPGroup    `yaml:"group"`
	BufferSizeKb           int               `yaml:"bufferSizeKb" default:"16384"`
	ConnectionBufferSizeKb uint              `yaml:"connectionBufferSizeKb" default:"20480"`
	ConnectionTimeout      time.Duration     `yaml:"connectionTimeout"`
	Listener               ConfigDCPListener `yaml:"listener"`
}

type ConfigAPI struct {
	Port    int  `yaml:"port" default:"8080"`
	Enabled bool `yaml:"enabled" default:"true"`
}

type ConfigMetric struct {
	Path             string  `yaml:"path" default:"/metrics"`
	AverageWindowSec float64 `yaml:"averageWindowSec" default:"10.0"`
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

type ConfigCheckpoint struct {
	Type      string        `yaml:"type" default:"auto"`
	AutoReset string        `yaml:"autoReset" default:"earliest"`
	Interval  time.Duration `yaml:"interval"`
	Timeout   time.Duration `yaml:"timeout"`
}

type ConfigHealthCheck struct {
	Enabled  bool          `yaml:"enabled" default:"true"`
	Interval time.Duration `yaml:"interval"`
	Timeout  time.Duration `yaml:"timeout"`
}

type ConfigRollbackMitigation struct {
	Enabled bool `yaml:"enabled" default:"true"`
}

type ConfigMetadata struct {
	Type     string            `yaml:"type" default:"couchbase"`
	ReadOnly bool              `json:"readOnly" default:"false"`
	Config   map[string]string `yaml:"config"`
}

type Config struct {
	LeaderElection     ConfigLeaderElection     `yaml:"leaderElector"`
	Metric             ConfigMetric             `yaml:"metric"`
	BucketName         string                   `yaml:"bucketName"`
	ScopeName          string                   `yaml:"scopeName" default:"_default"`
	CollectionNames    []string                 `yaml:"collectionNames"`
	Password           string                   `yaml:"password"`
	Username           string                   `yaml:"username"`
	SecureConnection   bool                     `yaml:"secureConnection"`
	RootCAPath         string                   `yaml:"rootCAPath"`
	Hosts              []string                 `yaml:"hosts"`
	Checkpoint         ConfigCheckpoint         `yaml:"checkpoint"`
	Dcp                ConfigDCP                `yaml:"dcp"`
	API                ConfigAPI                `yaml:"api"`
	HealthCheck        ConfigHealthCheck        `yaml:"healthCheck"`
	RollbackMitigation ConfigRollbackMitigation `yaml:"rollbackMitigation"`
	Metadata           ConfigMetadata           `yaml:"metadata"`
}

func (c *Config) IsCollectionModeEnabled() bool {
	return !(c.ScopeName == DefaultScopeName && len(c.CollectionNames) == 1 && c.CollectionNames[0] == DefaultCollectionName)
}

func (c *Config) IsCouchbaseMetadata() bool {
	return c.Metadata.Type == MetadataTypeCouchbase
}

func (c *Config) IsFileMetadata() bool {
	return c.Metadata.Type == MetadataTypeFile
}

func (c *Config) GetFileMetadata() string {
	var fileName string

	if _, ok := c.Metadata.Config[FileMetadataFileNameConfig]; ok {
		fileName = c.Metadata.Config[FileMetadataFileNameConfig]
	} else {
		err := errors.New("file metadata file name is not set")
		logger.ErrorLog.Printf("failed to get metadata file name: %v", err)
		panic(err)
	}

	if fileName == "" {
		err := errors.New("file metadata file name is empty")
		logger.ErrorLog.Printf("failed to get metadata file name: %v", err)
		panic(err)
	}

	return fileName
}

func (c *Config) GetCouchbaseMetadata() (string, string, string) {
	var bucket, scope, collection string

	if _, ok := c.Metadata.Config[CouchbaseMetadataBucketConfig]; ok {
		bucket = c.Metadata.Config[CouchbaseMetadataBucketConfig]
	} else {
		err := errors.New("couchbase metadata bucket name is not set")
		logger.ErrorLog.Printf("failed to get metadata bucket name: %v", err)
		panic(err)
	}

	if _, ok := c.Metadata.Config[CouchbaseMetadataScopeConfig]; ok {
		scope = c.Metadata.Config[CouchbaseMetadataScopeConfig]
	} else {
		scope = DefaultScopeName
	}

	if _, ok := c.Metadata.Config[CouchbaseMetadataCollectionConfig]; ok {
		collection = c.Metadata.Config[CouchbaseMetadataCollectionConfig]
	} else {
		collection = DefaultCollectionName
	}

	return bucket, scope, collection
}

func Options(opts *config.Options) {
	opts.ParseTime = true
	opts.Readonly = true
	opts.EnableCache = true
	opts.ParseDefault = true
}

func applyUnhandledDefaults(_config *Config) {
	if _config.Checkpoint.Interval == 0 {
		_config.Checkpoint.Interval = 20 * time.Second
	}

	if _config.Checkpoint.Timeout == 0 {
		_config.Checkpoint.Timeout = 5 * time.Second
	}

	if _config.HealthCheck.Interval == 0 {
		_config.HealthCheck.Interval = 20 * time.Second
	}

	if _config.HealthCheck.Timeout == 0 {
		_config.HealthCheck.Timeout = 5 * time.Second
	}

	if _config.Dcp.Group.Membership.RebalanceDelay == 0 {
		_config.Dcp.Group.Membership.RebalanceDelay = 20 * time.Second
	}

	if _config.Dcp.ConnectionTimeout == 0 {
		_config.Dcp.ConnectionTimeout = 5 * time.Second
	}

	if _config.CollectionNames == nil {
		_config.CollectionNames = []string{DefaultCollectionName}
	}
}

func NewConfig(name string, filePath string) *Config {
	conf := config.New(name).WithOptions(Options).WithDriver(yamlv3.Driver)

	err := conf.LoadFiles(filePath)
	if err != nil {
		logger.ErrorLog.Printf("cannot load config file: %v", err)
		panic(err)
	}

	_config := &Config{}
	err = conf.Decode(_config)

	if err != nil {
		logger.ErrorLog.Printf("cannot decode config file: %v", err)
		panic(err)
	}

	logger.Log.Printf("config loaded from file: %v", filePath)

	applyUnhandledDefaults(_config)

	return _config
}
