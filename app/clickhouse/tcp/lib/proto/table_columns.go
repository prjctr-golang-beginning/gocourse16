package proto

import (
	"fmt"

	"gocourse16/app/clickhouse/tcp/binary"
)

type TableColumns struct {
	First  string
	Second string
}

func (t *TableColumns) Decode(decoder *binary.Decoder, revision uint64) (err error) {
	if t.First, err = decoder.String(); err != nil {
		return err
	}
	if t.Second, err = decoder.String(); err != nil {
		return err
	}
	return nil
}

func (t *TableColumns) String() string {
	return fmt.Sprintf("first=%s, second=%s", t.First, t.Second)
}
