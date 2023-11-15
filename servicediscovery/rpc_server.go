package servicediscovery

import (
	"fmt"
	"net"
	"net/rpc"

	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp/logger"
)

type Server interface {
	Listen()
	Shutdown()
}

type server struct {
	listener net.Listener
	handler  *Handler
	port     int
}

type Handler struct {
	serviceDiscovery ServiceDiscovery
	myIdentity       *models.Identity
	port             int
}

func (rh *Handler) Ping(_ Ping, reply *Pong) error {
	*reply = Pong{
		From: rh.myIdentity,
	}

	return nil
}

func (rh *Handler) Register(payload Register, reply *bool) error {
	followerClient, err := NewClient(rh.port, rh.myIdentity, payload.Identity)
	if err != nil {
		*reply = false
		return err
	}

	followerService := NewService(followerClient, payload.Identity.Name, payload.Identity.ClusterJoinTime)
	rh.serviceDiscovery.Add(followerService)

	logger.Log.Debug("registered client %s", payload.Identity.Name)

	*reply = true

	return nil
}

func (rh *Handler) Rebalance(payload Rebalance, reply *bool) error {
	rh.serviceDiscovery.SetInfo(payload.MemberNumber, payload.TotalMembers)

	*reply = true

	return nil
}

func (s *server) Listen() {
	server := rpc.NewServer()

	err := server.Register(s.handler)
	if err != nil {
		logger.Log.Error("error while registering rpc handler: %s", err)
		panic(err)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		logger.Log.Error("error while listening rpc server: %s", err)
		panic(err)
	}

	s.listener = listener
	logger.Log.Info("rpc server started on port %d", s.port)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				logger.Log.Error("rpc server error: %s", err)

				break
			}

			go server.ServeConn(conn)
		}

		logger.Log.Info("rpc server stopped")
	}()
}

func (s *server) Shutdown() {
	err := s.listener.Close()
	if err != nil {
		logger.Log.Error("error while closing rpc server: %s", err)
	}
}

func NewServer(port int, myIdentity *models.Identity, serviceDiscovery ServiceDiscovery) Server {
	return &server{
		port: port,
		handler: &Handler{
			port:             port,
			myIdentity:       myIdentity,
			serviceDiscovery: serviceDiscovery,
		},
	}
}
