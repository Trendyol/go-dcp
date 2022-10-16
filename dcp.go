package main

type Dcp interface {
	StartAndWait()
	Close()
}

type dcp struct {
	client   Client
	metadata Metadata
	listener Listener
}

func (s *dcp) StartAndWait() {
	stream := NewStream(s.client, s.metadata, s.listener).Start()
	stream.Wait()
}

func (s *dcp) Close() {
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

	metadata := NewCBMetadata(client.GetAgent())

	return &dcp{
		client:   client,
		metadata: metadata,
		listener: listener,
	}, nil
}
