package remote

import (
	"context"
	"fmt"
	"gocourse16/app/clickhouse/tcp/lib/proto"
	"time"
)

// Connection::ping
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Client/Connection.cpp
func (c *connect) ping(ctx context.Context) (err error) {
	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(time.Time{})
	}
	c.debugf("[ping] -> ping")
	if err := c.encoder.Byte(proto.ClientPing); err != nil {
		return err
	}
	if err := c.encoder.Flush(); err != nil {
		return err
	}
	var packet byte
	for {
		if packet, err = c.decoder.ReadByte(); err != nil {
			return err
		}
		switch packet {
		case proto.ServerException:
			return c.exception()
		case proto.ServerProgress:
			if _, err = c.progress(); err != nil {
				return err
			}
		case proto.ServerPong:
			c.debugf("[ping] <- pong")
			return nil
		default:
			return fmt.Errorf("unexpected packet %d", packet)
		}
	}
}
