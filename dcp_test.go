package dcp

import (
	"os"
	"testing"
)

func TestNewDcpConfigWithEnvVariables(t *testing.T) {
	os.Setenv("DCP_USERNAME", "envUser")
	os.Setenv("DCP_PASSWORD", "envPass")
	os.Setenv("DCP_BUCKET_NAME", "envBucket")

	configContent := `
hosts: ["localhost:8091"]
username: ${DCP_USERNAME}
password: ${DCP_PASSWORD}
bucketName: ${DCP_BUCKET_NAME}
`
	tmpFile, err := os.CreateTemp("", "dcpConfig-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(configContent)); err != nil {
		t.Fatal(err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatal(err)
	}

	dcpConfig, err := newDcpConfig(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}

	if dcpConfig.Username != "envUser" {
		t.Errorf("expected username to be 'envUser', got '%s'", dcpConfig.Username)
	}
	if dcpConfig.Password != "envPass" {
		t.Errorf("expected password to be 'envPass', got '%s'", dcpConfig.Password)
	}
	if dcpConfig.BucketName != "envBucket" {
		t.Errorf("expected bucketName to be 'envBucket', got '%s'", dcpConfig.BucketName)
	}
}
