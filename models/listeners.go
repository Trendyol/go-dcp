package models

type ListenerContext struct {
	Commit func()
	Status Status
	Event  interface{}
}

func (ctx *ListenerContext) Ack() {
	ctx.Status = Ack
}

func (ctx *ListenerContext) Discard() {
	ctx.Status = Discard
}

type ListenerArgs struct {
	Event interface{}
}

type (
	InternalListener func(interface{})
	Listener         func(*ListenerContext)
	ListenerCh       chan ListenerArgs
)
