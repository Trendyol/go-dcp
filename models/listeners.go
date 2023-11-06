package models

type ListenerContext struct {
	Commit func()
	Event  interface{}
	Ack    func()
}

type ListenerArgs struct {
	Event interface{}
}

type DcpStreamEndContext struct {
	Err   error
	Event DcpStreamEnd
}

type (
	Listener      func(*ListenerContext)
	ListenerCh    chan ListenerArgs
	ListenerEndCh chan DcpStreamEndContext
)
