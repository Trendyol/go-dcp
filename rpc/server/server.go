package server

import (
	"fmt"
	"log"
	"net"

	"github.com/Trendyol/go-dcp-client/model"
	"github.com/Trendyol/go-dcp-client/rpc"
	"github.com/Trendyol/go-dcp-client/rpc/client"
	"github.com/Trendyol/go-dcp-client/servicediscovery"
	sdm "github.com/Trendyol/go-dcp-client/servicediscovery/model"

	pureRpc "net/rpc"
)

type Server interface {
	Listen()
	Shutdown()
}

type server struct {
	port     int
	listener net.Listener
	handler  *Handler
}

type Handler struct {
	port             int
	myIdentity       *model.Identity
	serviceDiscovery servicediscovery.ServiceDiscovery
}

func (rh *Handler) Ping(_ rpc.Ping, reply *rpc.Pong) error {
	*reply = rpc.Pong{
		From: *rh.myIdentity,
	}

	return nil
}

func (rh *Handler) Register(payload rpc.Register, reply *bool) error {
	followerClient, err := client.NewClient(rh.port, rh.myIdentity, &payload.Identity)
	if err != nil {
		*reply = false
		return err
	}

	followerService := sdm.NewService(followerClient, false, payload.Identity.Name)
	rh.serviceDiscovery.Add(followerService)

	log.Printf("registered client %s", payload.Identity.Name)

	*reply = true

	return nil
}

func (rh *Handler) Rebalance(payload rpc.Rebalance, reply *bool) error {
	rh.serviceDiscovery.SetInfo(payload.MemberNumber, payload.TotalMembers)

	*reply = true

	return nil
}

func (s *server) Listen() {
	server := pureRpc.NewServer()

	err := server.Register(s.handler)
	if err != nil {
		panic(err)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		panic(err)
	}

	s.listener = listener
	log.Printf("rpc server started on port %d", s.port)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("rpc server error: %v", err)

				break
			}

			go server.ServeConn(conn)
		}

		log.Printf("rpc server stopped")
	}()
}

func (s *server) Shutdown() {
	err := s.listener.Close()
	if err != nil {
		panic(err)
	}
}

func NewServer(port int, myIdentity *model.Identity, serviceDiscovery servicediscovery.ServiceDiscovery) Server {
	return &server{
		port: port,
		handler: &Handler{
			port:             port,
			myIdentity:       myIdentity,
			serviceDiscovery: serviceDiscovery,
		},
	}
}
