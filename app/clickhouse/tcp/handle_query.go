package tcp

import (
	"context"
	"errors"
	"fmt"
	"gocourse16/app/clickhouse/tcp/lib/proto"
	"gocourse16/app/log"
)

// Handle natively copied new instance of Handler to process conn
func (h Handler) query() error {
	q := proto.Query{}

	if err := q.Decode(h.decoder, h.target.ServerVersion().Revision); err != nil {
		return err
	}
	log.Infof("[query decoding] query=%s", q.Body)

	if err := h.readQueryData(); err != nil {
		return err
	}

	ex, err := h.createExtractor(q.Body, h.handshake.Database)
	if err != nil {
		return err
	}

	remote, err := h.target.Acquire(
		ex,
		h.handshake.Database,
		h.handshake.Username,
		h.handshake.Password,
	)
	if err != nil {
		return err
	}
	err = remote.BroadcastTo(context.Background(), h.encoder)
	if err != nil {
		return err
	}

	b := h.decoder.FlushBufBytes()
	err = remote.Send(context.TODO(), b)

	return err
}

func (h *Handler) readQueryData() error {
	for {
		packet, err := h.decoder.ReadByte() // read afterQuery packet
		if err != nil {
			return err
		}
		switch packet {
		case proto.ClientData:
			blockName, err := h.decoder.String()
			if err != nil {
				return err
			}
			_, err = h.readData(proto.ClientData, true) // read some block, according packet (???)

			if blockName == `` {
				return nil
			}
		default:
			return errors.New(fmt.Sprintf("[unexpected packet %d] from client", h.route))
		}
	}
}
