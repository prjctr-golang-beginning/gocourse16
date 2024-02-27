package tcp

import (
	"gocourse16/app/clickhouse/tcp/lib/proto"
	"gocourse16/app/log"
)

// Handle natively copied new instance of Handler to process conn
func (h *Handler) Handshake() error {
	clientHs := &proto.ClientHandshake{}
	if err := clientHs.Decode(h.decoder); err != nil {
		return err
	}
	h.handshake = clientHs

	log.Infof("[handshake] <- %s", clientHs)

	serverHs := h.target.ServerVersion()
	serverHs.Name = proto.ClientName
	serverHs.DisplayName = proto.ClientDisplayName

	if err := serverHs.Encode(h.encoder); err != nil {
		return err
	}

	if err := h.Send(); err != nil {
		return err
	}
	h.revision = serverHs.Revision // TODO: server revision hack (crunch?)

	log.Infof("[handshake] -> %s", serverHs)

	return nil
}
