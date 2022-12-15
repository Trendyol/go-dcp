package servicediscovery

type Service struct {
	Client   Client
	IsLeader bool
	Name     string
}

func NewService(client Client, isLeader bool, name string) *Service {
	return &Service{
		Client:   client,
		IsLeader: isLeader,
		Name:     name,
	}
}
