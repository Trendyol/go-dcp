package godcpclient

import (
	"os"
	"os/signal"
	"syscall"
)

type Dcp interface {
	StartAndWait()
	Close()
}

type dcp struct {
	client   Client
	metadata Metadata
	listener Listener
	stream   Stream
	config   Config
}

func (s *dcp) StartAndWait() {
	s.stream = NewStream(s.client, s.metadata, s.config, s.listener)
	s.stream.Start()

	cancelCh := make(chan os.Signal, 1)
	signal.Notify(cancelCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		s.stream.Wait()
		close(cancelCh)
	}()

	<-cancelCh
}

func (s *dcp) Close() {
	s.stream.Save()
	s.stream.Stop()
	s.client.Close()
	s.client.DcpClose()
}

func NewDcp(configPath string, listener Listener) (Dcp, error) {
	config := NewConfig(configPath)

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
