package app

import (
	"net"
	"time"
)

type conn2Proc struct {
	conn     net.Conn
	expired  bool
	accepted chan struct{}
}

func ConnWithTimeout(c net.Conn, to float64, expiredNotify chan<- struct{}) *conn2Proc {
	c2p := &conn2Proc{
		conn:     c,
		accepted: make(chan struct{}),
	}

	go c2p.windUp(to, expiredNotify)

	return c2p
}

// windUp starts timeout countdown
func (c *conn2Proc) windUp(t float64, tellAboutExpired chan<- struct{}) {
	select {
	case <-time.After(time.Second * time.Duration(t)):
		c.expired = true
		tellAboutExpired <- struct{}{}
	case <-c.accepted:

	}
}

func (c *conn2Proc) Accept() {
	c.accepted <- struct{}{}
}
