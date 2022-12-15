package model

import (
	"os"

	"github.com/Trendyol/go-dcp-client/model"
)

func NewIdentityFromEnv() *model.Identity {
	return &model.Identity{
		IP:   os.Getenv("POD_IP"),
		Name: os.Getenv("POD_NAME"),
	}
}
