package http

import (
	"gocourse16/app/driver"
	"net"
)

func NewHandler(rtp driver.RemoteTargetsPool, ef driver.ExtractorFactory) *Handler {
	return &Handler{target: rtp, createExtractor: ef}
}

type Handler struct {
	target   driver.RemoteTargetsPool
	revision uint64

	createExtractor driver.ExtractorFactory
	route           byte
}

// CloneWith natively copied new instance of Handler to process remote2
func (s Handler) CloneWith(conn net.Conn) driver.Handler {
	return nil //&s
}

func (s *Handler) Stop() error {
	return nil
}

func (s *Handler) Handle() error { // TODO: wrap consumer to process connection
	return nil
}
