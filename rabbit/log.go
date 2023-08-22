package rabbit

import (
	"log"
)

var _ Logger = (*Log)(nil)

type Logger interface {
	Info(args ...interface{})
	Infof(template string, args ...interface{})
	Fatalf(template string, args ...interface{})
}

type Log struct{}

var logger = NewLogger()

func (l *Log) Info(args ...interface{}) {
	log.Println(args...)
}
func (l *Log) Infof(template string, args ...interface{}) {
	log.Printf(template, args...)
}

func (l *Log) Fatalf(template string, args ...interface{}) {
	log.Fatalf(template, args...)
}

func NewLogger() Logger {
	return &Log{}
}
