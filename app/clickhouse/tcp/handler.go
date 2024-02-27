package tcp

import (
	"errors"
	"fmt"
	"gocourse16/app/clickhouse/tcp/binary"
	"gocourse16/app/clickhouse/tcp/io"
	"gocourse16/app/clickhouse/tcp/lib/proto"
	"gocourse16/app/driver"
	"gocourse16/app/log"
	"net"
)

var eofError = errors.New(`EOF`)

func NewHandler(rtp driver.RemoteTargetsPool, ef driver.ExtractorFactory, conn net.Conn) *Handler {
	h := &Handler{target: rtp, createExtractor: ef}
	h.SetConn(conn)

	return h
}

func (h *Handler) SetConn(conn net.Conn) {
	h.conn = conn
	h.stream = io.NewStream(conn)
	h.decoder = binary.NewDecoder(h.stream)
	h.encoder = binary.NewEncoder(h.stream)
}

type Handler struct { // not happy with this struct
	target driver.RemoteTargetsPool

	conn        net.Conn
	stream      *io.Stream
	encoder     *binary.Encoder
	decoder     *binary.Decoder
	closed      bool
	compression bool

	handshake *proto.ClientHandshake

	revision uint64

	createExtractor driver.ExtractorFactory
	route           byte
}

func (h *Handler) Stop() error {
	if h.closed {
		return nil
	}
	h.closed = true
	h.encoder = nil
	h.decoder = nil
	if err := h.stream.Close(); err != nil {
		return err
	}
	if err := h.conn.Close(); err != nil {
		return err
	}
	return nil
}

func (h *Handler) readData(packet byte, compressible bool) (*proto.Block, error) {
	if compressible && h.compression {
		h.stream.Compress(true)
		defer h.stream.Compress(false)
	}
	var block proto.Block
	if err := block.Decode(h.decoder, h.handshake.ProtocolVersion); err != nil {
		return nil, err
	}
	block.Packet = packet
	log.Infof("[read data] compression=%t. block: columns=%d, rows=%d", h.compression, len(block.Columns), block.Rows())
	return &block, nil
}

func (h *Handler) Send() error {
	return h.encoder.Flush()
}

func (h *Handler) ReplyWithException(code int32, name, message string) error {
	var err error

	exp := &proto.Exception{
		Code:    code, // come up with correct service codes
		Name:    name,
		Message: fmt.Sprintf("Original: %s", message),
	}
	if err = exp.Encode(h.encoder); err != nil {
		return err
	}
	if err = h.Send(); err != nil {
		return err
	}

	return nil
}

func (h *Handler) Handle() error { // TODO: wrap consumer to process connection
	for {
		err := h.handle()

		if err != nil {
			switch e := err.(type) {
			case *proto.Exception:
				if err = e.Encode(h.encoder); err != nil {
					return err
				}
				if err = h.Send(); err != nil {
					return err
				}
			default:
				if errors.As(err, &eofError) { // success return
					log.Info(`[end of stream]`)
					return nil
				}
				if err = h.ReplyWithException(1, "Undefined error", e.Error()); err != nil {
					return err
				}
			}
		}
	}
}

func (h *Handler) handle() error {
	defer h.decoder.ResetBuf()

	var err error

	h.route, err = h.decoder.ReadByte()
	if err != nil {
		return err
	}

	switch h.route {
	case proto.ClientHello:
		err = h.Handshake()
	case proto.ClientQuery:
		err = h.query()
		if err == nil {

		}
	case proto.ClientCancel:
		panic("Cancel processing not defined")
	case proto.ClientPing:
		err = h.ping()
	default:
		err = errors.New(fmt.Sprintf("unexpected packet [%d] from server", h.route))
	}

	return err
}
