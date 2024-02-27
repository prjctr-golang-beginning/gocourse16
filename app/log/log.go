package log

import (
	"fmt"
	"go.uber.org/zap"
	log2 "log"
)

var debug bool

func MustInitLogger(env string, debugEnabled bool) {
	var (
		l   *zap.Logger
		err error
	)

	switch env {
	default:
		fallthrough
	case `prod`:
		l, err = zap.NewProduction()
	case `dev`:
		l, err = zap.NewDevelopment()
	}

	if err != nil || l == nil {
		panic("Logger not defined: " + err.Error())
	}

	debug = debugEnabled
	log = &CustomLog{l /*.With(zap.String(`app`, `ch-repro`))*/}
}

func WithCategory(val string) Logger {
	return &CustomLog{
		log.Logger.With(zap.String(`category`, val)),
	}
}

func Info(msg string /*, fields ...Field*/) {
	//nf := make([]zap.Field, 0, len(fields))
	//for i := range fields {
	//	nf = append(nf, zap.Field(fields[i]))
	//}
	log.Info(msg /*, nf...*/)
}

// Info formats message
func Infof(msg string, args ...any) {
	log.Info(fmt.Sprintf(msg, args...))
}

func Warn(msg string) {
	log.Warn(msg)
}

func Warnf(msg string, args ...any) {
	log.Warn(fmt.Sprintf(msg, args...))
}

func Debug(msg string) {
	if debug {
		log.Debug(msg)
	}
}

func Debugf(msg string, args ...any) {
	if debug {
		log.Debugf(fmt.Sprintf(msg, args...))
	}
}

func Error(msg string) {
	log.Error(msg)
}

func Errorf(msg string, args ...any) {
	log.Error(fmt.Sprintf(msg, args...))
}

func DPanic(msg string) {
	log.DPanic(msg)
}

func Panic(msg string) {
	log.Panic(msg)
}

func Panicf(msg string, args ...any) {
	log.Panic(fmt.Sprintf(msg, args...))
}

func Fatal(msg string) {
	log.Fatal(msg)
	log2.Fatal()
}

// Fatalf formats message
func Fatalf(msg string, args ...any) {
	log.Fatal(fmt.Sprintf(msg, args...))
}
