package rabbit

import (
	"fmt"
	"log"
	"sync/atomic"
)

var _ Logger = (*Log)(nil)

type Logger interface {
	Info(args ...any)
	Infof(template string, args ...any)
	Errorf(template string, args ...any)
}

type Log struct{}

type loggerHolder struct {
	logger Logger
}

type formattedLogger interface {
	Infof(template string, args ...any)
	Errorf(template string, args ...any)
}

type infoLogger interface {
	Info(args ...any)
}

type adaptedLogger struct {
	formatted formattedLogger
	info      infoLogger
}

var loggerValue atomic.Value

func (l *Log) Info(args ...any) {
	log.Println(args...)
}

func (l *Log) Infof(template string, args ...any) {
	log.Printf(template, args...)
}

func (l *Log) Errorf(template string, args ...any) {
	log.Printf(template, args...)
}

func NewLogger() Logger {
	return &Log{}
}

func SetLogger(l Logger) {
	if l == nil {
		return
	}
	loggerValue.Store(loggerHolder{logger: l})
}

func currentLogger() Logger {
	holder, _ := loggerValue.Load().(loggerHolder)
	logger := holder.logger
	if logger == nil {
		logger = NewLogger()
		loggerValue.Store(loggerHolder{logger: logger})
	}

	return logger
}

func SetExternalLogger(l any) bool {
	if l == nil {
		return false
	}

	if logger, ok := l.(Logger); ok {
		SetLogger(logger)
		return true
	}

	formatted, ok := l.(formattedLogger)
	if !ok {
		return false
	}

	var info infoLogger
	if typed, ok := l.(infoLogger); ok {
		info = typed
	}

	SetLogger(adaptedLogger{
		formatted: formatted,
		info:      info,
	})
	return true
}

func (l adaptedLogger) Info(args ...any) {
	if l.info != nil {
		l.info.Info(args...)
		return
	}

	l.formatted.Infof("%s", fmt.Sprint(args...))
}

func (l adaptedLogger) Infof(template string, args ...any) {
	l.formatted.Infof(template, args...)
}

func (l adaptedLogger) Errorf(template string, args ...any) {
	l.formatted.Errorf(template, args...)
}
