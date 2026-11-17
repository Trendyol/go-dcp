package logger

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/rs/zerolog"
)

const (
	ERROR = "error"
	WARN  = "warn"
	INFO  = "info"
	DEBUG = "debug"
	TRACE = "trace"
)

type Logger struct {
	z      zerolog.Logger
	custom bool
}

var Log = &Logger{z: newZerolog(os.Stdout, zerolog.InfoLevel)}

func newZerolog(w io.Writer, level zerolog.Level) zerolog.Logger {
	return zerolog.New(w).Level(level).With().Timestamp().Logger()
}

func (l *Logger) Trace(msg string, args ...any) {
	l.write(l.z.Trace(), msg, args)
}

func (l *Logger) Debug(msg string, args ...any) {
	l.write(l.z.Debug(), msg, args)
}

func (l *Logger) Info(msg string, args ...any) {
	l.write(l.z.Info(), msg, args)
}

func (l *Logger) Warn(msg string, args ...any) {
	l.write(l.z.Warn(), msg, args)
}

func (l *Logger) Error(msg string, args ...any) {
	l.write(l.z.Error(), msg, args)
}

func (l *Logger) write(event *zerolog.Event, msg string, args []any) {
	if event == nil {
		return
	}
	fields(event, args).Msg(msg)
}

func IsCustom() bool {
	return Log.custom
}

func Set(l zerolog.Logger) {
	Log.z = l
	Log.custom = true
	gocbcore.SetLogger(&goCbCoreLogger{maxLevel: zeroToCore(l.GetLevel())})
}

func InitDefaultLogger(logLevel string) {
	level, err := ParseLevel(logLevel)
	if err != nil {
		Log.Error("error while logger parse level", "error", err, "level", logLevel)
		panic(err)
	}

	Log.z = newZerolog(os.Stdout, level)
	Log.custom = false
	gocbcore.SetLogger(&goCbCoreLogger{maxLevel: zeroToCore(level)})
}

func ParseLevel(level string) (zerolog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case ERROR:
		return zerolog.ErrorLevel, nil
	case WARN, "warning":
		return zerolog.WarnLevel, nil
	case "", INFO:
		return zerolog.InfoLevel, nil
	case DEBUG:
		return zerolog.DebugLevel, nil
	case TRACE:
		return zerolog.TraceLevel, nil
	default:
		return 0, errors.New("unknown log level: " + level)
	}
}

func fields(event *zerolog.Event, args []any) *zerolog.Event {
	for i := 0; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return event.Interface("args", args[i])
		}

		key, ok := args[i].(string)
		if !ok {
			event = event.Interface("invalidKey", args[i])
			event = field(event, "invalidValue", args[i+1])
			continue
		}
		event = field(event, key, args[i+1])
	}
	return event
}

func field(event *zerolog.Event, key string, value any) *zerolog.Event {
	switch v := value.(type) {
	case error:
		return event.AnErr(key, v)
	case string:
		return event.Str(key, v)
	case bool:
		return event.Bool(key, v)
	case int:
		return event.Int(key, v)
	case int8:
		return event.Int8(key, v)
	case int16:
		return event.Int16(key, v)
	case int32:
		return event.Int32(key, v)
	case int64:
		return event.Int64(key, v)
	case uint:
		return event.Uint(key, v)
	case uint8:
		return event.Uint8(key, v)
	case uint16:
		return event.Uint16(key, v)
	case uint32:
		return event.Uint32(key, v)
	case uint64:
		return event.Uint64(key, v)
	case float32:
		return event.Float32(key, v)
	case float64:
		return event.Float64(key, v)
	case time.Duration:
		return event.Dur(key, v)
	case time.Time:
		return event.Time(key, v)
	case fmt.Stringer:
		return event.Stringer(key, v)
	default:
		return event.Interface(key, v)
	}
}

func zeroToCore(level zerolog.Level) gocbcore.LogLevel {
	switch level {
	case zerolog.TraceLevel:
		return gocbcore.LogMaxVerbosity
	case zerolog.DebugLevel:
		return gocbcore.LogDebug
	case zerolog.InfoLevel:
		return gocbcore.LogInfo
	case zerolog.WarnLevel:
		return gocbcore.LogWarn
	case zerolog.ErrorLevel, zerolog.FatalLevel, zerolog.PanicLevel:
		return gocbcore.LogError
	case zerolog.NoLevel, zerolog.Disabled:
		return gocbcore.LogLevel(-1)
	default:
		return gocbcore.LogError
	}
}

var coreToZero = map[gocbcore.LogLevel]zerolog.Level{
	gocbcore.LogError:        zerolog.ErrorLevel,
	gocbcore.LogWarn:         zerolog.WarnLevel,
	gocbcore.LogInfo:         zerolog.InfoLevel,
	gocbcore.LogDebug:        zerolog.DebugLevel,
	gocbcore.LogTrace:        zerolog.TraceLevel,
	gocbcore.LogSched:        zerolog.TraceLevel,
	gocbcore.LogMaxVerbosity: zerolog.TraceLevel,
}

type goCbCoreLogger struct {
	maxLevel gocbcore.LogLevel
}

func (l *goCbCoreLogger) Log(level gocbcore.LogLevel, _ int, format string, v ...any) error {
	if level > l.maxLevel {
		return nil
	}

	zLevel, ok := coreToZero[level]
	if !ok {
		zLevel = zerolog.TraceLevel
	}

	Log.z.WithLevel(zLevel).Str("component", "gocbcore").Msg(fmt.Sprintf(format, v...))
	return nil
}
