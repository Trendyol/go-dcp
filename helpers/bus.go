package helpers

type Listener = func(event interface{})

type Bus interface {
	Emit(eventName string, event interface{})
	Subscribe(eventName string, listener Listener)
}

type bus struct {
	listeners map[string][]Listener
}

func (i *bus) Emit(eventName string, event interface{}) {
	if listeners, ok := i.listeners[eventName]; ok {
		for _, listener := range listeners {
			listener(event)
		}
	}
}

func (i *bus) Subscribe(eventName string, listener Listener) {
	if listeners, ok := i.listeners[eventName]; ok {
		i.listeners[eventName] = append(listeners, listener)
	} else {
		i.listeners[eventName] = []Listener{listener}
	}
}

func NewBus() Bus {
	return &bus{
		listeners: map[string][]Listener{},
	}
}
