package logger

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

var Log Logger

const (
	ERROR = "ERROR"
	WARN  = "WARN"
	INFO  = "INFO"
	DEBUG = "DEBUG"
	TRACE = "TRACE"
)

type Logger interface {
	Trace(message string, args ...interface{})
	Debug(message string, args ...interface{})
	Info(message string, args ...interface{})
	Warn(message string, args ...interface{})
	Error(message string, args ...interface{})
	Log(level string, message string, args ...interface{})
}

type Loggers struct {
	Logrus *logrus.Logger
}

func (loggers *Loggers) Trace(message string, args ...interface{}) {
	loggers.Log(TRACE, message, args...)
}

func (loggers *Loggers) Debug(message string, args ...interface{}) {
	loggers.Log(DEBUG, message, args...)
}

func (loggers *Loggers) Info(message string, args ...interface{}) {
	loggers.Log(INFO, message, args...)
}

func (loggers *Loggers) Warn(message string, args ...interface{}) {
	loggers.Log(WARN, message, args...)
}

func (loggers *Loggers) Error(message string, args ...interface{}) {
	loggers.Log(ERROR, message, args...)
}

func (loggers *Loggers) Log(level string, message string, args ...interface{}) {
	logLevel, _ := logrus.ParseLevel(level)
	loggers.Logrus.Log(logLevel, fmt.Sprintf(message, args...))
}

func InitDefaultLogger(logLevel string) {
	logger := logrus.New()

	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		panic(err)
	}
	logger.SetLevel(level)

	formatter := &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyMsg: "message",
		},
	}

	logger.SetFormatter(formatter)
	Log = &Loggers{
		Logrus: logger,
	}
}
