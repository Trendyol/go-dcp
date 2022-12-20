package servicediscovery

import (
	"fmt"
	"net"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/identity"

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
	myIdentity       *identity.Identity
	serviceDiscovery ServiceDiscovery
}

func (rh *Handler) Ping(_ Ping, reply *Pong) error {
	*reply = Pong{
		From: *rh.myIdentity,
	}

	return nil
}

func (rh *Handler) Register(payload Register, reply *bool) error {
	followerClient, err := NewClient(rh.port, rh.myIdentity, &payload.Identity)
	if err != nil {
		*reply = false
		return err
	}

	followerService := NewService(followerClient, false, payload.Identity.Name)
	rh.serviceDiscovery.Add(followerService)

	logger.Debug("registered client %s", payload.Identity.Name)

	*reply = true

	return nil
}

func (rh *Handler) Rebalance(payload Rebalance, reply *bool) error {
	rh.serviceDiscovery.SetInfo(payload.MemberNumber, payload.TotalMembers)

	*reply = true

	return nil
}

func (s *server) Listen() {
	server := pureRpc.NewServer()

	err := server.Register(s.handler)
	if err != nil {
		logger.Panic(err, "error while registering rpc handler")
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		logger.Panic(err, "error while listening rpc server")
	}

	s.listener = listener
	logger.Debug("rpc server started on port %d", s.port)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				logger.Error(err, "rpc server error")

				break
			}

			go server.ServeConn(conn)
		}

		logger.Debug("rpc server stopped")
	}()
}

func (s *server) Shutdown() {
	err := s.listener.Close()
	if err != nil {
		logger.Panic(err, "error while closing rpc server")
	}
}

func NewServer(port int, myIdentity *identity.Identity, serviceDiscovery ServiceDiscovery) Server {
	return &server{
		port: port,
		handler: &Handler{
			port:             port,
			myIdentity:       myIdentity,
			serviceDiscovery: serviceDiscovery,
		},
	}
}
