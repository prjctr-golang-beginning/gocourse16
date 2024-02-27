package remote

import (
	"context"
	"fmt"
	"gocourse16/app/clickhouse/tcp/lib/proto"
	"io"
	"time"
)

type onProcess struct {
	data          func(*proto.Block)
	logs          func([]Log)
	progress      func(*Progress)
	profileInfo   func(*ProfileInfo)
	profileEvents func([]ProfileEvent)
}

func (c *connect) firstBlock(ctx context.Context, on *onProcess) (*proto.Block, error) {
	for {
		select {
		case <-ctx.Done():
			c.cancel()
			return nil, ctx.Err()
		default:
		}
		packet, err := c.decoder.ReadByte()
		if err != nil {
			return nil, err
		}
		switch packet {
		case proto.ServerData:
			return c.readData(packet, true)
		case proto.ServerEndOfStream:
			c.debugf("[end of stream]")
			return nil, io.EOF
		default:
			if err := c.handle(packet, on, nil); err != nil {
				return nil, err
			}
		}
	}
}

func (c *connect) process(ctx context.Context, on *onProcess, consumer io.Writer) error {
	for {
		select {
		case <-ctx.Done():
			c.cancel()
			return ctx.Err()
		default:
		}
		packet, err := c.decoder.ReadByte()
		if err != nil {
			return err
		}
		switch packet {
		case proto.ServerEndOfStream:
			c.debugf("[end of stream]")
			if consumer != nil { // TODO: make it gracefully
				b := c.decoder.FlushBufBytes()
				_, err = consumer.Write(b)
			}
			return err
		}
		if err := c.handle(packet, on, consumer); err != nil {
			return err
		}
	}
}

func (c *connect) handle(packet byte, on *onProcess, consumer io.Writer) error {
	switch packet {
	case proto.ServerData, proto.ServerTotals, proto.ServerExtremes:
		block, err := c.readData(packet, true)
		if err != nil {
			return err
		}
		if block.Rows() != 0 && on.data != nil {
			on.data(block)
		}
	case proto.ServerException:
		return c.exception()
	case proto.ServerProfileInfo:
		var info proto.ProfileInfo
		if err := info.Decode(c.decoder, c.revision); err != nil {
			return err
		}
		c.debugf("[profile info] %s", &info)
		on.profileInfo(&info)
	case proto.ServerTableColumns:
		var info proto.TableColumns
		if err := info.Decode(c.decoder, c.revision); err != nil {
			return err
		}
		c.debugf("[table columns]")
	case proto.ServerProfileEvents:
		events, err := c.profileEvents()
		if err != nil {
			return err
		}
		on.profileEvents(events)
	case proto.ServerLog:
		logs, err := c.logs()
		if err != nil {
			return err
		}
		on.logs(logs)
	case proto.ServerProgress:
		progress, err := c.progress()
		if err != nil {
			return err
		}
		c.debugf("[progress] %s", progress)
		on.progress(progress)
	default:
		return &OpError{
			Op:  "process",
			Err: fmt.Errorf("unexpected packet %d", packet),
		}
	}
	b := c.decoder.FlushBufBytes()
	if consumer != nil { // TODO: make it gracefully
		_, err := consumer.Write(b)
		return err
	}
	return nil
}

func (c *connect) cancel() error {
	c.conn.SetDeadline(time.Now().Add(2 * time.Second))
	c.debugf("[cancel]")
	c.closed = true
	if err := c.encoder.Uvarint(proto.ClientCancel); err == nil {
		return err
	}
	return c.encoder.Flush()
}
