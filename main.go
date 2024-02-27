package main

import (
	"context"
	"fmt"
	"gocourse16/app"
	app2 "gocourse16/app"
	"gocourse16/app/clickhouse/tcp"
	"gocourse16/app/driver"
	"gocourse16/app/extension"
	"gocourse16/app/extractor"
	"gocourse16/app/http"
	"gocourse16/app/log"
	"gopkg.in/yaml.v3"
	"net"
	"os"
	"os/signal"
	"sync"
)

var (
	cfg *app.Config
)

func init() {
	filename := fmt.Sprintf("config.%s.yml", getEnv())
	configContent, _ := os.ReadFile(filename)
	cfgToDo := app2.Config{}
	err := yaml.Unmarshal(configContent, &cfgToDo)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	cfg = &cfgToDo

	log.MustInitLogger(cfg.Env, cfg.Debug)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	remotePool := app.NewRemotePool(
		ctx,
		cfg.Targets,
		app.RoundRobinRouteRule,
		app.WeightRouteRule)
	remotePool.Extend(&extension.SelectOnly{})

	if err := remotePool.Init(); err != nil {
		log.Errorf("Interrogate problem: %s", err)
	}

	wg.Add(2)
	go func(wg *sync.WaitGroup) {
		remotePool.Interrogate()
		wg.Done()
	}(wg)
	go func(wg *sync.WaitGroup) {
		alive := remotePool.HealthCheck()
		log.Infof("Alive remote instances: %d", alive)
		wg.Done()
	}(wg)

	go remotePool.Interrogating()
	go remotePool.HealthChecking()

	for i := range cfg.Listen.Tcp {
		go func(srv *app.Router) {
			err := srv.Start()
			if err != nil {
				log.Fatal(err.Error())
			}
		}(app.NewRouter(ctx,
			cfg.Listen.Tcp[i], TcpHandler(remotePool)))
	}

	go http.Serve(wg, remotePool,
		cfg.Http.Port)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	for sig := range c {
		log.Infof("Signal received %v, stopping and exiting...", sig)
		cancel()
		return
	}
}

func TcpHandler(rp *app.RemotePool) func(net.Conn) driver.Handler {
	return func(conn net.Conn) driver.Handler {
		return tcp.NewHandler(rp, extractor.NewExtractorRegex, conn)
	}
}

func getEnv() string {
	if env, ok := os.LookupEnv(`env`); ok {
		return env
	}

	return `dev`
}
