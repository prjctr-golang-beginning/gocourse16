package app

import (
	"net"
)

func (r *Router) routeNoLimit(ln net.Listener) {
	r.log.Infof(
		"Start listening TCP on %s:%s starts in NO LIMIT mode",
		r.listen.Host,
		r.listen.Port,
	)

	for {
		if conn, err := acceptConn(ln); err != nil {
			r.log.Error(err.Error())
		} else {
			if !r.filter.IsAllowed(conn.RemoteAddr().String()) {
				r.notAllowedException(conn)
				continue
			}
			go r.handle(conn /*, r.connectionId*/) // TODO: make timeout for handling connection??
		}
	}
}
