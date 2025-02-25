package logger

import (
	"fmt"

	"github.com/couchbase/gocbcore/v10"

	"github.com/sirupsen/logrus"
)

var Log Logger

const (
	ERROR = "error"
	WARN  = "warn"
	INFO  = "info"
	DEBUG = "debug"
	TRACE = "trace"
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

	formatter := &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyMsg: "message",
		},
	}

	logger.SetFormatter(formatter)

	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		logger.Errorf("error while logger parse level, err: %v", err)
		panic(err)
	}
	logger.SetLevel(level)
	gocbcore.SetLogger(&goCbCoreLogger{level: dcpToCore[level]})

	Log = &Loggers{
		Logrus: logger,
	}
}

var coreToDcp = map[gocbcore.LogLevel]string{
	gocbcore.LogError:        ERROR,
	gocbcore.LogWarn:         WARN,
	gocbcore.LogInfo:         DEBUG,
	gocbcore.LogDebug:        DEBUG,
	gocbcore.LogTrace:        TRACE,
	gocbcore.LogSched:        TRACE,
	gocbcore.LogMaxVerbosity: TRACE,
}

var dcpToCore = map[logrus.Level]gocbcore.LogLevel{
	logrus.ErrorLevel: gocbcore.LogError,
	logrus.WarnLevel:  gocbcore.LogWarn,
	logrus.InfoLevel:  gocbcore.LogInfo,
	logrus.DebugLevel: gocbcore.LogDebug,
	logrus.TraceLevel: gocbcore.LogMaxVerbosity,
}

type goCbCoreLogger struct {
	level gocbcore.LogLevel
}

func (l *goCbCoreLogger) Log(level gocbcore.LogLevel, _ int, format string, v ...interface{}) error {
	if level > l.level {
		return nil
	}
	msg := fmt.Sprintf(format, v...)
	Log.Log(coreToDcp[level], "gocbcore - %s", msg)
	return nil
}
