package helpers

type Listener = func(event interface{})

type Bus interface {
	Emit(eventName string, event interface{})
	Subscribe(eventName string, listener Listener)
}

type bus struct {
	listeners map[string]Listener
}

func (i *bus) Emit(eventName string, event interface{}) {
	if listener, ok := i.listeners[eventName]; ok {
		listener(event)
	}
}

func (i *bus) Subscribe(eventName string, listener Listener) {
	i.listeners[eventName] = listener
}

func NewBus() Bus {
	return &bus{
		listeners: map[string]Listener{},
	}
}
