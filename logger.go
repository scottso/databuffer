package databuffer

import (
	"log"
	"os"
)

type Logger interface {
	Errorf(format string, v ...any)
	Infof(format string, v ...any)
	Warnf(format string, v ...any)
	Debugf(format string, v ...any)
}

func createLogger() *logger {
	l := &logger{l: log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds)}
	return l
}

var _ Logger = (*logger)(nil)

type logger struct {
	l *log.Logger
}

func (l *logger) Errorf(format string, v ...any) {
	l.output(format, v...)
}

func (l *logger) Infof(format string, v ...any) {
	l.output(format, v...)
}

func (l *logger) Warnf(format string, v ...any) {
	l.output(format, v...)
}

func (l *logger) Debugf(format string, v ...any) {
	l.output(format, v...)
}

func (l *logger) output(format string, v ...any) {
	if len(v) == 0 {
		l.l.Print(format)
		return
	}
	l.l.Printf(format, v...)
}
