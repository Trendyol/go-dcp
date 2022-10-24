package godcpclient

import (
	"os"
	"os/signal"
	"syscall"
)

type Dcp interface {
	Start()
	Close()
}

type dcp struct {
	client   Client
	metadata Metadata
	listener Listener
	stream   Stream
	config   Config
	api      Api
}

func (s *dcp) Start() {
	s.stream = NewStream(s.client, s.metadata, s.config, s.listener)
	s.stream.Open()

	s.api = NewApi(s.config, s.stream.GetObserver())
	s.api.Listen()

	cancelCh := make(chan os.Signal, 1)
	signal.Notify(cancelCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		s.stream.Wait()
		close(cancelCh)
	}()

	<-cancelCh
}

func (s *dcp) Close() {
	s.api.Shutdown()
	s.stream.Save()
	s.stream.Close()
	s.client.Close()
	s.client.DcpClose()
}

func NewDcp(configPath string, listener Listener) (Dcp, error) {
	config := NewConfig(Name, configPath)

	client := NewClient(config)

	err := client.DcpConnect()

	if err != nil {
		return nil, err
	}

	err = client.Connect()

	if err != nil {
		return nil, err
	}

	metadata := NewCBMetadata(client.GetAgent(), config)

	return &dcp{
		client:   client,
		metadata: metadata,
		listener: listener,
		config:   config,
	}, nil
}
