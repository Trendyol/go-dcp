package servicediscovery

import (
	"fmt"
	"time"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/identity"

	"github.com/avast/retry-go/v4"

	pureRpc "net/rpc"
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
	client         *pureRpc.Client
	myIdentity     *identity.Identity
	targetIdentity *identity.Identity
	port           int
	connected      bool
}

func (c *client) connect() error {
	return retry.Do(
		func() error {
			connectAddress := fmt.Sprintf("%s:%d", c.targetIdentity.IP, c.port)
			client, err := pureRpc.Dial("tcp", connectAddress)
			if err != nil {
				return err
			}

			c.client = client
			c.connected = true
			logger.Debug("connected to %s as rpc", c.targetIdentity.Name)

			return nil
		},
		retry.Attempts(3),
		retry.DelayType(retry.FixedDelay),
		retry.Delay(1*time.Second),
	)
}

func (c *client) Close() error {
	if !c.IsConnected() {
		return nil
	}

	logger.Debug("closing rpc client %s", c.targetIdentity.Name)

	c.connected = false
	err := c.client.Close()
	c.client = nil

	return err
}

func (c *client) IsConnected() bool {
	return c.connected
}

func (c *client) Reconnect() error {
	logger.Debug("reconnecting rpc client %s", c.targetIdentity.Name)
	return c.connect()
}

func (c *client) Ping() error {
	return retry.Do(
		func() error {
			var reply Pong

			return c.client.Call("Handler.Ping", Ping{From: *c.myIdentity}, &reply)
		},
		retry.Attempts(3),
		retry.DelayType(retry.FixedDelay),
		retry.Delay(100*time.Millisecond),
	)
}

func (c *client) Register() error {
	return retry.Do(
		func() error {
			var reply bool

			return c.client.Call("Handler.Register", Register{From: *c.myIdentity, Identity: *c.myIdentity}, &reply)
		},
		retry.Attempts(3),
		retry.DelayType(retry.FixedDelay),
		retry.Delay(100*time.Millisecond),
	)
}

func (c *client) Rebalance(memberNumber int, totalMembers int) error {
	return retry.Do(
		func() error {
			var reply bool

			return c.client.Call(
				"Handler.Rebalance",
				Rebalance{From: *c.myIdentity, MemberNumber: memberNumber, TotalMembers: totalMembers},
				&reply,
			)
		},
		retry.Attempts(3),
		retry.DelayType(retry.FixedDelay),
		retry.Delay(100*time.Millisecond),
	)
}

func NewClient(port int, myIdentity *identity.Identity, targetIdentity *identity.Identity) (Client, error) {
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
