package log

import (
	"fmt"
	"go.uber.org/zap"
	log2 "log"
)

type CustomLog struct {
	*zap.Logger
}

type Logger interface {
	Debug(msg string)
	Debugf(msg string, args ...any)
	Info(msg string)
	Infof(msg string, args ...any)
	Warn(msg string)
	Error(msg string)
	Errorf(msg string, args ...any)
	DPanic(msg string)
	Panic(msg string)
	Fatal(msg string)
}

var log *CustomLog

func (l *CustomLog) Info(msg string) {
	l.Logger.Info(msg)
}

// Info formats message
func (l *CustomLog) Infof(msg string, args ...any) {
	l.Logger.Info(fmt.Sprintf(msg, args...))
}

func (l *CustomLog) Warn(msg string) {
	l.Logger.Warn(msg)
}

func (l *CustomLog) Warnf(msg string, args ...any) {
	l.Logger.Warn(fmt.Sprintf(msg, args...))
}

func (l *CustomLog) Error(msg string) {
	l.Logger.Error(msg)
}

func (l *CustomLog) Errorf(msg string, args ...any) {
	l.Logger.Error(fmt.Sprintf(msg, args...))
}

func (l *CustomLog) DPanic(msg string) {
	l.Logger.DPanic(msg)
}

func (l *CustomLog) Panic(msg string) {
	l.Logger.Panic(msg)
}

func (l *CustomLog) Fatal(msg string) {
	l.Logger.Fatal(msg)
	log2.Fatal()
}

// Fatalf formats message
func (l *CustomLog) Fatalf(msg string, args ...any) {
	l.Logger.Fatal(fmt.Sprintf(msg, args...))
}

func (l *CustomLog) Debug(msg string) {
	l.Logger.Debug(msg)
}

func (l *CustomLog) Debugf(msg string, args ...any) {
	l.Logger.Debug(fmt.Sprintf(msg, args...))
}
