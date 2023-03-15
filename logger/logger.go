package logger

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func Info(format string, v ...interface{}) {
	log.Info().Msgf(format, v...)
}

func Debug(format string, v ...interface{}) {
	log.Debug().Msgf(format, v...)
}

func Error(err error, format string, v ...interface{}) {
	log.Error().Err(err).Msgf(format, v...)
}

func Trace(format string, v ...interface{}) {
	log.Trace().Msgf(format, v...)
}

func Panic(err error, format string, v ...interface{}) {
	log.Panic().Err(err).Msgf(format, v...)
}

func SetLevel(level zerolog.Level) {
	zerolog.SetGlobalLevel(level)
}
