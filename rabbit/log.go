package rabbit

import "log"

type Logger struct{}

var logger = NewLogger()

func (l *Logger) Info(args ...interface{}) {
	log.Println(args...)
}

func (l *Logger) Errorf(template string, args ...interface{}) {
	log.Fatalf(template, args...)

}

func NewLogger() *Logger {
	return &Logger{}
}
