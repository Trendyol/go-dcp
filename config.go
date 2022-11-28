package godcpclient

import (
	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yamlv3"
	"log"
	"time"
)

type ConfigDCPGroupMembership struct {
	Type         string `json:"type" yaml:"type"`
	MemberNumber int    `yaml:"memberNumber"`
	TotalMembers int    `yaml:"totalMembers"`
}

type ConfigDCPGroup struct {
	Name       string                   `yaml:"name"`
	Membership ConfigDCPGroupMembership `yaml:"membership"`
}

type ConfigDCP struct {
	ConnectTimeout             time.Duration  `yaml:"connectTimeout"`
	FlowControlBuffer          int            `yaml:"flowControlBuffer"`
	PersistencePollingInterval time.Duration  `yaml:"persistencePollingInterval"`
	Group                      ConfigDCPGroup `yaml:"group"`
}

type ConfigAPI struct {
	Port int `yaml:"port"`
}

type ConfigMetric struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
}

type Config struct {
	Hosts          []string      `yaml:"hosts"`
	Username       string        `yaml:"username"`
	Password       string        `yaml:"password"`
	BucketName     string        `yaml:"bucketName"`
	UserAgent      string        `yaml:"userAgent"`
	Compression    bool          `yaml:"compression"`
	MetadataBucket string        `yaml:"metadataBucket"`
	ConnectTimeout time.Duration `yaml:"connectTimeout"`
	Dcp            ConfigDCP     `yaml:"dcp"`
	Api            ConfigAPI     `yaml:"api"`
	Metric         ConfigMetric  `yaml:"metric"`
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
