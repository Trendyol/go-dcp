package model

import (
	"github.com/Trendyol/go-dcp-client/model"
	"os"
)

func NewIdentityFromEnv() *model.Identity {
	return &model.Identity{
		Ip:   os.Getenv("POD_IP"),
		Name: os.Getenv("POD_NAME"),
	}
}
