package http

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gocourse16/app/driver"
	"gocourse16/app/log"
	"net/http"
	"sync"
)

func Serve(wg *sync.WaitGroup, a driver.Aliver, port string) {
	wg.Wait()

	http.HandleFunc("/", Hello)

	http.Handle("/metrics", promhttp.Handler())

	http.Handle("/probe/startup", &startupProbe{a})
	http.Handle("/probe/liveness", &livenessProbe{a})
	http.Handle("/probe/readiness", &readinessProbe{a})

	log.Infof(`HTTP listen on port %s`, port)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("HTTP server is down: %s", err)
	}
}

func Hello(rw http.ResponseWriter, _ *http.Request) {
	_, _ = rw.Write([]byte("Hello"))
}
