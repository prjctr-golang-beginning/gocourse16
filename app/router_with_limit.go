package app

import (
	"net"
	"sync"
)

func (r *Router) routeWithLimit(ln net.Listener) {
	r.log.Infof(
		"Start listening TCP on %s:%s in LIMIT mode: max concurrency conns: %d, queue size: %d",
		r.listen.Host,
		r.listen.Port,
		r.opts.ConcurrencyLimit,
		r.opts.MaxQueueSize,
	)

	r.conn2Proc = make(chan *conn2Proc, r.opts.MaxQueueSize)
	r.someoneExpired = make(chan struct{}, r.opts.MaxQueueSize) // just in case
	wg := r.prepareHandlers()
	wg.Wait()

	for {
		if conn, err := acceptConn(ln); err != nil {
			r.log.Error(err.Error())
		} else {
			if !r.filter.IsAllowed(conn.RemoteAddr().String()) {
				r.notAllowedException(conn)
				continue
			}
			ql, qc := len(r.conn2Proc), cap(r.conn2Proc)
			if ql < qc {
				r.conn2Proc <- ConnWithTimeout(conn, r.opts.MaxWaitTime, r.someoneExpired)
			} else {
				r.queueOverflowException(conn)
				if err := conn.Close(); err != nil {
					r.log.Errorf(`Exception about waiting queue not sent: %s`, err.Error())
				}
			}
		}
	}
}

func (r *Router) prepareHandlers() *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	wg.Add(r.opts.ConcurrencyLimit)
	for i := 0; i < r.opts.ConcurrencyLimit; i++ {
		go func(conn <-chan *conn2Proc, wg *sync.WaitGroup) {
			wg.Done()
			for {
				select {
				case newOne := <-conn:
					newOne.Accept()
					if newOne.expired {
						r.waitingTimeoutException(newOne.conn)
						continue
					}
					r.handle(newOne.conn)
				case <-r.ctx.Done():
					return
				}
			}
		}(r.conn2Proc, wg)
	}

	wg.Add(1)
	go func(wg *sync.WaitGroup) { // recycle bin for consumer connections
		wg.Done()
		for {
			<-r.someoneExpired
			select {
			case newOne := <-r.conn2Proc:
				if newOne.expired {
					r.waitingTimeoutException(newOne.conn)
					continue
				}
				go r.handle(newOne.conn)
			case <-r.ctx.Done():
				return
			}
		}
	}(wg)

	return wg
}
