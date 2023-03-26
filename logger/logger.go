package logger

import "fmt"

type Logger interface {
	Printf(string, ...interface{})
}

type DefaultLogger struct{}

func (d *DefaultLogger) Printf(msg string, args ...interface{}) {
	fmt.Printf(msg+"\n", args...)
}

var (
	Log      Logger = &DefaultLogger{}
	ErrorLog Logger = &DefaultLogger{}
)

type LogFunc func(string, ...interface{})

func (f LogFunc) Printf(msg string, args ...interface{}) { f(msg, args...) }

func SetLogger(logger Logger) {
	Log = logger
}

func SetErrorLogger(logger Logger) {
	ErrorLog = logger
}
