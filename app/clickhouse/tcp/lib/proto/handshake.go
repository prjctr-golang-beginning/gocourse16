package proto

import (
	"fmt"
	"gocourse16/app/clickhouse/tcp/binary"
	"gocourse16/app/clickhouse/tcp/lib/timezone"
	"time"
)

const (
	ClientName        = "TCP Reverse proxy"
	ClientDisplayName = "TCP Reverse proxy"

	ClientVersionMajor       = 1
	ClientVersionMinor       = 1
	ClientTCPProtocolVersion = DBMS_TCP_PROTOCOL_VERSION
)

type ClientHandshake struct {
	Name            string
	ProtocolVersion uint64
	Version         struct {
		Major uint64
		Minor uint64
		Patch uint64
	}
	Timezone *time.Location
	Database string
	Username string
	Password string
}

func (ClientHandshake) Encode(encoder *binary.Encoder) error {
	if err := encoder.String(ClientName); err != nil {
		return err
	}
	if err := encoder.Uvarint(ClientVersionMajor); err != nil {
		return err
	}
	if err := encoder.Uvarint(ClientVersionMinor); err != nil {
		return err
	}
	return encoder.Uvarint(ClientTCPProtocolVersion)
}

func (ClientHandshake) String() string {
	return fmt.Sprintf("%s %d.%d.%d", ClientName, ClientVersionMajor, ClientVersionMinor, ClientTCPProtocolVersion)
}

func (cl *ClientHandshake) Decode(decoder *binary.Decoder) error {
	var err error
	if cl.Name, err = decoder.String(); err != nil {
		return err
	}
	if cl.Version.Major, err = decoder.Uvarint(); err != nil {
		return err
	}
	if cl.Version.Minor, err = decoder.Uvarint(); err != nil {
		return err
	}
	if cl.ProtocolVersion, err = decoder.Uvarint(); err != nil {
		return err
	}
	if cl.Database, err = decoder.String(); err != nil {
		return err
	}
	if cl.Username, err = decoder.String(); err != nil {
		return err
	}
	if cl.Password, err = decoder.String(); err != nil {
		return err
	}
	return err
}

type ServerHandshake struct {
	Name        string
	DisplayName string
	Revision    uint64
	Version     struct {
		Major uint64
		Minor uint64
		Patch uint64
	}
	Timezone *time.Location
}

func (srv *ServerHandshake) Encode(encoder *binary.Encoder) (err error) {
	if err = encoder.Byte(ServerHello); err != nil {
		return fmt.Errorf("could not read server name: %v", err)
	}
	if err = encoder.String(srv.Name); err != nil {
		return fmt.Errorf("could not read server name: %v", err)
	}
	if err = encoder.Uvarint(srv.Version.Major); err != nil {
		return fmt.Errorf("could not read server major version: %v", err)
	}
	if err = encoder.Uvarint(srv.Version.Minor); err != nil {
		return fmt.Errorf("could not read server minor version: %v", err)
	}
	if err = encoder.Uvarint(srv.Revision); err != nil {
		return fmt.Errorf("could not read server revision: %v", err)
	}
	if srv.Revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
		err = encoder.String("UTC")
		if err != nil {
			return fmt.Errorf("could not read server timezone: %v", err)
		}
	}
	if srv.Revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
		if err = encoder.String(srv.DisplayName); err != nil {
			return fmt.Errorf("could not read server display name: %v", err)
		}
	}
	if srv.Revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		if err = encoder.Uvarint(srv.Version.Patch); err != nil {
			return fmt.Errorf("could not read server patch: %v", err)
		}
	} else {
		srv.Version.Patch = srv.Revision
	}
	return nil
}

func (srv *ServerHandshake) Decode(decoder *binary.Decoder) (err error) {
	if srv.Name, err = decoder.String(); err != nil {
		return fmt.Errorf("could not read server name: %v", err)
	}
	if srv.Version.Major, err = decoder.Uvarint(); err != nil {
		return fmt.Errorf("could not read server major version: %v", err)
	}
	if srv.Version.Minor, err = decoder.Uvarint(); err != nil {
		return fmt.Errorf("could not read server minor version: %v", err)
	}
	if srv.Revision, err = decoder.Uvarint(); err != nil {
		return fmt.Errorf("could not read server revision: %v", err)
	}
	if srv.Revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
		name, err := decoder.String()
		if err != nil {
			return fmt.Errorf("could not read server timezone: %v", err)
		}
		if srv.Timezone, err = timezone.Load(name); err != nil {
			return fmt.Errorf("could not load time location: %v", err)
		}
	}
	if srv.Revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
		if srv.DisplayName, err = decoder.String(); err != nil {
			return fmt.Errorf("could not read server display name: %v", err)
		}
	}
	if srv.Revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		if srv.Version.Patch, err = decoder.Uvarint(); err != nil {
			return fmt.Errorf("could not read server patch: %v", err)
		}
	} else {
		srv.Version.Patch = srv.Revision
	}
	return nil
}

func (srv ServerHandshake) String() string {
	return fmt.Sprintf("%s (%s) server version %d.%d.%d revision %d (timezone %s)", srv.Name, srv.DisplayName,
		srv.Version.Major,
		srv.Version.Minor,
		srv.Version.Patch,
		srv.Revision,
		srv.Timezone,
	)
}
