package extension

import (
	"gocourse16/app/clickhouse/tcp/lib/proto"
	"gocourse16/app/driver"
)

type SelectOnly struct {
}

var notSelectException = &proto.Exception{
	Code:    01, // TODO: make codes system
	Name:    "Enter point restriction",
	Message: "Only SELECT available",
}

func (s *SelectOnly) Use(stmt driver.SqlPartsExtractor) error {
	if !stmt.IsSelect() {
		return notSelectException
	}

	return nil
}
