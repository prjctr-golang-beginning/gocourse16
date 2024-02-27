package app

import (
	"context"
	"go.uber.org/atomic"
	"gocourse16/app/clickhouse/tcp/remote"
	"gocourse16/app/driver"
	stringsStd "strings"
	"time"
)

type (
	RemoteMaster struct {
		opts remote.Options
		// remote connection, owned by router (router config credentials)
		ownConn       driver.Conn
		alive         *atomic.Bool
		tables        []RemoteTable
		specialTables specialTables
		serverVersion *remote.ServerVersion
	}
	RemoteTable struct {
		Name string `ch:"table"`
		B    uint64 `ch:"b"`
	}
)

func NewRemoteMaster(db, user, pass string, addr ...string) *RemoteMaster {
	if len(addr) < 1 {
		panic("no onw address to dial")
	}
	return &RemoteMaster{
		alive: atomic.NewBool(true),
		opts: remote.Options{
			Addr: addr,
			Auth: remote.Auth{
				Database: db,
				Username: user,
				Password: pass,
			},
			Settings: remote.Settings{
				"max_execution_time": 60,
			},
			DialTimeout: 3 * time.Second,
		}}
}

func (s *RemoteMaster) Acquire(db, user, pass string) (driver.Conn, error) {
	opts := remote.Options{
		Addr: s.opts.Addr,
		Auth: remote.Auth{
			Database: db,
			Username: user,
			Password: pass,
		},
		Settings:    s.opts.Settings,
		DialTimeout: s.opts.DialTimeout,
	}

	return remote.Open(&opts)
}

func (s *RemoteMaster) Ping(ctx context.Context) error {
	return s.ownConn.Ping(ctx)
}

func (s *RemoteMaster) Alive() bool {
	return s.alive.Load()
}

// PushTable adds table in list for further consumer query routing
func (s *RemoteMaster) PushTable(t RemoteTable) {
	t.Name = stringsStd.ToLower(t.Name)
	if s.SkipOnBlackList(t.Name) || s.SkipOnWhiteList(t.Name) {
		return
	}
	s.tables = append(s.tables, t)
}

// SkipOnWhiteList defines is given table isn't contained in preconfigured white list
func (s *RemoteMaster) SkipOnWhiteList(tName string) bool {
	if !s.specialTables.IsWhite() {
		return false // due to not used
	}

	return !s.specialTables.Contains(tName)
}

// SkipOnBlackList defines is given table is contained in preconfigured black list
func (s *RemoteMaster) SkipOnBlackList(tName string) bool {
	if defaultBlackList.Contains(tName) {
		return true
	}

	if !s.specialTables.IsBlack() {
		return false // due to not used
	}

	return s.specialTables.Contains(tName)
}
