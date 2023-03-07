package servicediscovery

type Service struct {
	Client Client
	Name   string
}

func NewService(client Client, name string) *Service {
	return &Service{
		Client: client,
		Name:   name,
	}
}
