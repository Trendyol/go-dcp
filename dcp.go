package godcpclient

import (
	"github.com/Trendyol/go-dcp-client/helpers"
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

	if s.config.Metric.Enabled {
		s.api = NewApi(s.config, s.stream.GetObserver())
		s.api.Listen()
	}

	cancelCh := make(chan os.Signal, 1)
	signal.Notify(cancelCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		s.stream.Wait()
		close(cancelCh)
	}()

	<-cancelCh
}

func (s *dcp) Close() {
	if s.config.Metric.Enabled {
		s.api.Shutdown()
	}

	s.stream.Save()
	s.stream.Close()
	s.client.DcpClose()
	s.client.Close()
}

func NewDcp(configPath string, listener Listener) (Dcp, error) {
	config := NewConfig(helpers.Name, configPath)

	client := NewClient(config)

	err := client.Connect()

	if err != nil {
		return nil, err
	}

	err = client.DcpConnect()

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
