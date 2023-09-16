package logger

import (
	"fmt"
	"github.com/sirupsen/logrus"
)

var Log Logger

const (
	FATAL = "FATAL"
	ERROR = "ERROR"
	WARN  = "WARN"
	INFO  = "INFO"
	DEBUG = "DEBUG"
	TRACE = "TRACE"
)

type Logger interface {
	Debug(message string, args ...interface{})
	Info(message string, args ...interface{})
	Warn(message string, args ...interface{})
	Error(message string, args ...interface{})
	Fatal(message string, args ...interface{})
	Trace(message string, args ...interface{})
	Log(level string, message string, args ...interface{})
}

type Loggers struct {
	logrus  *logrus.Logger
	appName string
}

func (loggers *Loggers) Debug(message string, args ...interface{}) {
	loggers.LogCtx(DEBUG, message, args...)
}

func (loggers *Loggers) Info(message string, args ...interface{}) {
	loggers.LogCtx(INFO, message, args...)
}

func (loggers *Loggers) Warn(message string, args ...interface{}) {
	loggers.LogCtx(WARN, message, args...)
}

func (loggers *Loggers) Error(message string, args ...interface{}) {
	loggers.LogCtx(ERROR, message, args...)
}

func (loggers *Loggers) Fatal(message string, args ...interface{}) {
	loggers.LogCtx(FATAL, message, args...)
}

func (loggers *Loggers) Trace(message string, args ...interface{}) {
	loggers.LogCtx(TRACE, message, args...)
}

func (loggers *Loggers) Log(level string, message string, args ...interface{}) {
	loggers.LogCtx(TRACE, message, args...)
}

func (loggers *Loggers) LogCtx(level string, message string, args ...interface{}) {
	fields := logrus.Fields{
		"app-name": loggers.appName,
	}

	logLevel, _ := logrus.ParseLevel(level)
	loggers.logrus.WithFields(fields).Log(logLevel, fmt.Sprintf(message, args...))
}

func InitLogger(logLevel string, appName string) {
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
		logrus:  logger,
		appName: appName,
	}
}
