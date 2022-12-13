package model

import rpcClient "github.com/Trendyol/go-dcp-client/rpc/client"

type Service struct {
	Client   rpcClient.Client
	IsLeader bool
	Name     string
}

func NewService(client rpcClient.Client, isLeader bool, name string) *Service {
	return &Service{
		Client:   client,
		IsLeader: isLeader,
		Name:     name,
	}
}
