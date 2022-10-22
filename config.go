package godcpclient

import (
	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yamlv3"
	"log"
	"time"
)

type ConfigDCPGroupMembership struct {
	MemberNumber int `yaml:"memberNumber"`
	TotalMembers int `yaml:"totalMembers"`
}

type ConfigDCPGroup struct {
	Name       string                   `yaml:"name"`
	Membership ConfigDCPGroupMembership `yaml:"membership"`
}

type ConfigDCP struct {
	ConnectTimeout    time.Duration  `yaml:"connectTimeout"`
	FlowControlBuffer int            `yaml:"flowControlBuffer"`
	Group             ConfigDCPGroup `yaml:"group"`
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
}

func NewConfig(filePath string) Config {
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

	log.Printf("Config loaded from file: %v", filePath)

	return _config
}
