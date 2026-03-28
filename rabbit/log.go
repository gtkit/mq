package rabbit

import (
	"fmt"
	"log"
	"sync/atomic"
)

var _ Logger = (*Log)(nil)

// Logger 定义了库内部使用的最小日志接口。
// 调用方可以实现该接口后通过 SetLogger 注入自己的日志实例。
type Logger interface {
	Info(args ...any)
	Infof(template string, args ...any)
	Errorf(template string, args ...any)
}

// Log 是基于标准库 log 的默认日志实现。
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

// NewLogger 返回默认日志实现。
func NewLogger() Logger {
	return &Log{}
}

// SetLogger 注入一个实现了 Logger 接口的日志实例。
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

// SetExternalLogger 注入一个外部日志实例。
// 如果传入对象直接实现了 Logger，会直接使用；
// 否则只要实现了 Infof 和 Errorf，也会被自动适配。
// 该方法适合直接接入 github.com/gtkit/logger 这类项目级 logger。
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
