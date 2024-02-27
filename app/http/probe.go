package http

import (
	"gocourse16/app/driver"
	"net/http"
)

type startupProbe struct {
	a driver.Aliver
}

func (s *startupProbe) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	switch {
	case s.a.AliveNum() < 1:
		rw.WriteHeader(500)
		_, _ = rw.Write([]byte("NOT READY"))
	default:
		rw.WriteHeader(200)
		_, _ = rw.Write([]byte("OK"))
	}
}

type livenessProbe struct {
	a driver.Aliver
}

func (s *livenessProbe) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	p := &startupProbe{s.a}
	p.ServeHTTP(rw, r)
}

type readinessProbe struct {
	a driver.Aliver
}

func (s *readinessProbe) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	p := &startupProbe{s.a}
	p.ServeHTTP(rw, r)
}
