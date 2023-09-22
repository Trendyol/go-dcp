package servicediscovery

import (
	"fmt"
	"net/rpc"
	"time"

	"github.com/Trendyol/go-dcp/helpers"

	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp/logger"
)

type Client interface {
	Close() error
	Ping() error
	Register() error
	IsConnected() bool
	Reconnect() error
	Rebalance(memberNumber int, totalMembers int) error
}

type client struct {
	client         *rpc.Client
	myIdentity     *models.Identity
	targetIdentity *models.Identity
	port           int
	connected      bool
}

func (c *client) connect() error {
	return helpers.Retry(
		func() error {
			connectAddress := fmt.Sprintf("%s:%d", c.targetIdentity.IP, c.port)
			client, err := rpc.Dial("tcp", connectAddress)
			if err != nil {
				return err
			}

			c.client = client
			c.connected = true
			logger.Log.Info("connected to %s as rpc", c.targetIdentity.Name)

			return nil
		},
		3,
		1*time.Second,
	)
}

func (c *client) Close() error {
	if !c.IsConnected() {
		return nil
	}

	logger.Log.Info("closing rpc client %s", c.targetIdentity.Name)

	c.connected = false
	err := c.client.Close()
	c.client = nil

	return err
}

func (c *client) IsConnected() bool {
	return c.connected
}

func (c *client) Reconnect() error {
	logger.Log.Info("reconnecting rpc client %s", c.targetIdentity.Name)
	return c.connect()
}

func (c *client) Ping() error {
	return helpers.Retry(
		func() error {
			var reply Pong

			return c.client.Call("Handler.Ping", Ping{From: c.myIdentity}, &reply)
		},
		3,
		100*time.Millisecond,
	)
}

func (c *client) Register() error {
	return helpers.Retry(
		func() error {
			var reply bool

			return c.client.Call("Handler.Register", Register{From: c.myIdentity, Identity: c.myIdentity}, &reply)
		},
		3,
		100*time.Millisecond,
	)
}

func (c *client) Rebalance(memberNumber int, totalMembers int) error {
	return helpers.Retry(
		func() error {
			var reply bool

			return c.client.Call(
				"Handler.Rebalance",
				Rebalance{From: c.myIdentity, MemberNumber: memberNumber, TotalMembers: totalMembers},
				&reply,
			)
		},
		3,
		100*time.Millisecond,
	)
}

func NewClient(port int, myIdentity *models.Identity, targetIdentity *models.Identity) (Client, error) {
	client := &client{
		port:           port,
		myIdentity:     myIdentity,
		targetIdentity: targetIdentity,
	}

	err := client.connect()
	if err != nil {
		return nil, err
	}

	return client, nil
}
