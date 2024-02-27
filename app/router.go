package app

import (
	"context"
	"errors"
	"fmt"
	"gocourse16/app/driver"
	"gocourse16/app/log"
	"net"
)

type Router struct {
	ctx     context.Context
	listen  Addr
	opts    NetworkOpts
	handler driver.HandlerFactory
	log     log.Logger
	filter  ConnFilter

	conn2Proc      chan *conn2Proc
	someoneExpired chan struct{}

	connectionId  uint64
	shutDownAsked bool
}

func NewRouter(ctx context.Context, c NetworkOpts, h driver.HandlerFactory) *Router {
	return &Router{
		ctx:     ctx,
		listen:  c.Addr,
		filter:  NewConnFilter(c.Allowed),
		opts:    c,
		handler: h,
		log:     log.WithCategory(`router`),
	}
}

func (r *Router) Start() error {
	ln, err := net.Listen(`tcp`, fmt.Sprintf("%s:%s", r.listen.Host, r.listen.Port))
	if err != nil {
		return err
	}

	go func() {
		r.log.Info("Waiting for shut down signal ^C")
		<-r.ctx.Done()
		r.shutDownAsked = true
		r.log.Info("Shut down signal received, closing connections...")
		if err := ln.Close(); err != nil {
			r.log.Infof(err.Error())
		}
	}()

	if r.opts.ConcurrencyLimit < 1 {
		r.routeNoLimit(ln)
	} else {
		r.routeWithLimit(ln)
	}

	return nil
}

func (r *Router) waitingTimeoutException(conn net.Conn) {
	if err := r.handler(conn).ReplyWithException(2, `Waiting in queue timeout expired`, `All connections are busy`); err != nil {
		r.log.Errorf(`Exception about timeout not sent: %s`, err.Error())
	}
}

func (r *Router) queueOverflowException(conn net.Conn) {
	if err := r.handler(conn).ReplyWithException(2, `Waiting queue is overflow`, `All connections are busy`); err != nil {
		r.log.Errorf(`Exception about timeout not sent: %s`, err.Error())
	}
}

func (r *Router) notAllowedException(conn net.Conn) {
	if err := r.handler(conn).ReplyWithException(2, `Waiting in queue timeout expired`, `All connections are busy`); err != nil {
		r.log.Errorf(`Exception about timeout not sent: %s`, err.Error())
	}
}

func (r *Router) handle(conn net.Conn /*, connectionId uint64*/) {
	defer func() {
		err := conn.Close()
		if err != nil {
			r.log.Error(err.Error())
		}
	}()

	h := r.handler(conn)
	go func(h driver.Handler) {
		<-r.ctx.Done()
		r.log.Info("Shut down signal received, Stopping handler...")
		if err := h.Stop(); err != nil {
			r.log.Error(err.Error())
		}
	}(h)
	if err := h.Handle(); err != nil {
		log.Errorf("Handle error: %s", err)
	}
}

func acceptConn(ln net.Listener) (net.Conn, error) {
	conn, err := ln.Accept()
	log.Debugf("Connection accepted: %s\n", conn.RemoteAddr())
	if err != nil {
		return nil, err
	}

	//r.connectionId += 1
	if err != nil {
		//	if r.shutDownAsked { // TODO: think about
		//		r.log.Infof("Shutdown asked [%d]", r.connectionId)
		//		break
		//	}
		return nil, errors.New(fmt.Sprintf("Failed to accept new remote2: [%d] %s" /*r.connectionId*/, 1, err.Error()))
	}

	return conn, nil
}
