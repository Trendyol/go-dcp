package client

import (
	"fmt"
	"log"
	"time"

	"github.com/Trendyol/go-dcp-client/model"
	"github.com/Trendyol/go-dcp-client/rpc"
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
	port           int
	connected      bool
	myIdentity     *model.Identity
	targetIdentity *model.Identity
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
			log.Printf("connected to %s as rpc", c.targetIdentity.Name)

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

	log.Printf("closing rpc client %s", c.targetIdentity.Name)

	c.connected = false
	err := c.client.Close()
	c.client = nil

	return err
}

func (c *client) IsConnected() bool {
	return c.connected
}

func (c *client) Reconnect() error {
	log.Printf("reconnecting rpc client %s", c.targetIdentity.Name)
	return c.connect()
}

func (c *client) Ping() error {
	return retry.Do(
		func() error {
			var reply rpc.Pong

			return c.client.Call("Handler.Ping", rpc.Ping{From: *c.myIdentity}, &reply)
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

			return c.client.Call("Handler.Register", rpc.Register{From: *c.myIdentity, Identity: *c.myIdentity}, &reply)
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
				rpc.Rebalance{From: *c.myIdentity, MemberNumber: memberNumber, TotalMembers: totalMembers},
				&reply,
			)
		},
		retry.Attempts(3),
		retry.DelayType(retry.FixedDelay),
		retry.Delay(100*time.Millisecond),
	)
}

func NewClient(port int, myIdentity *model.Identity, targetIdentity *model.Identity) (Client, error) {
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
