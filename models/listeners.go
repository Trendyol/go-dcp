package models

type ListenerContext struct {
	Commit func()
	Event  interface{}
	Ack    func()
}

type ListenerArgs struct {
	Event interface{}
}

type (
	Listener   func(*ListenerContext)
	ListenerCh chan ListenerArgs
)
