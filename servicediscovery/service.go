package servicediscovery

type Service struct {
	Client   Client
	Name     string
	IsLeader bool
}

func NewService(client Client, isLeader bool, name string) *Service {
	return &Service{
		Client:   client,
		IsLeader: isLeader,
		Name:     name,
	}
}
