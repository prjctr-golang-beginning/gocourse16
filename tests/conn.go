package tests

import (
	"database/sql"
	"fmt"
	"gocourse16/app/clickhouse/tcp/remote"
	"time"
)

const host = `127.0.0.1`
const port = `9000`
const user = ``
const pass = ``

type Row struct {
	Id   int `ch:"id"`
	Name int `ch:"name"`
}

type Ch struct {
	Host string
	Port string
	User string
	Pass string
}

func CreateConn() *sql.DB {
	conn := remote.OpenDB(&remote.Options{
		Addr: []string{fmt.Sprintf("%s:%s",
			host,
			port)},
		Auth: remote.Auth{
			Database: "default",
			Username: user,
			Password: pass,
		},
		Settings: remote.Settings{
			"max_execution_time": 99999,
		},
		DialTimeout: 150 * time.Second,
		Debug:       true,
	})
	conn.SetMaxIdleConns(5)
	conn.SetMaxOpenConns(10)
	conn.SetConnMaxLifetime(time.Hour)

	return conn
}
