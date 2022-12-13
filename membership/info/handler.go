package info

type OnModelChangeType = func(new *Model)

type Handler interface {
	OnModelChange(new *Model)
	Subscribe(listener OnModelChangeType)
}

type handler struct {
	listeners []OnModelChangeType
}

func (i *handler) OnModelChange(new *Model) {
	for _, listener := range i.listeners {
		listener(new)
	}
}

func (i *handler) Subscribe(listener OnModelChangeType) {
	i.listeners = append(i.listeners, listener)
}

func NewHandler(listeners ...OnModelChangeType) Handler {
	return &handler{
		listeners: listeners,
	}
}
