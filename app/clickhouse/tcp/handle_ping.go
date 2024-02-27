package tcp

import (
	"gocourse16/app/clickhouse/tcp/lib/proto"
)

func (h Handler) ping() error {
	_, err := h.encoder.Write([]byte{proto.ServerPong})

	return err
}
