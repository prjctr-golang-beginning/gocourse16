package driver

import (
	"context"
	"gocourse16/app/clickhouse/tcp/lib/driver"
	"io"
	"net"
)

const (
	Tcp  = `tcp`
	Http = `http`
)

type (
	ExtractorFactory func(sql, defaultDb string) (SqlPartsExtractor, error)
	HandlerFactory   func(net.Conn) Handler

	Conn interface {
		//Contributors() []string
		ServerVersion() (*driver.ServerVersion, error) // TODO: replace with interface
		//Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error
		Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error)
		//QueryRow(ctx context.Context, query string, args ...interface{}) Row
		//PrepareBatch(ctx context.Context, query string) (Batch, error)
		Send(ctx context.Context, packet []byte) error
		BroadcastTo(ctx context.Context, consumer io.Writer) error
		//AsyncInsert(ctx context.Context, query string, wait bool) error
		Ping(context.Context) error
		//Stats() Stats
		Close() error
	}
	Handler interface {
		Handle() error
		Stop() error
		ReplyWithException(int32, string, string) error
	}
	RemoteTargetsPool interface {
		Acquire(SQLQuery SqlPartsExtractor, db, user, pass string) (Conn, error)
		ServerVersion() *driver.ServerVersion // TODO: replace with interface
	}
	Aliver interface {
		AliveNum() int
	}
	Stream interface {
		io.ReadWriteCloser
		Compress(v bool)
		Flush() error
	}
	Extension interface {
		Use(stmt SqlPartsExtractor) error
	}
	SqlPartsExtractor interface {
		IsSelect() bool
		UsedTables() []string
	}
)
