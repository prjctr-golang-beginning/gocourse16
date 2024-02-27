package column

import (
	"fmt"
	"reflect"
	"strings"

	"gocourse16/app/clickhouse/tcp/binary"
)

type Type string

func (t Type) params() string {
	switch start, end := strings.Index(string(t), "("), strings.LastIndex(string(t), ")"); {
	case len(t) == 0, start <= 0, end <= 0, end < start:
		return ""
	default:
		return string(t[start+1 : end])
	}
}

type Error struct {
	ColumnType string
	Err        error
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.ColumnType, e.Err)
}

type ColumnConverterError struct {
	Op       string
	Hint     string
	From, To string
}

func (e *ColumnConverterError) Error() string {
	var hint string
	if len(e.Hint) != 0 {
		hint += ". " + e.Hint
	}
	return fmt.Sprintf("clickhouse [%s]: converting %s to %s is unsupported%s", e.Op, e.From, e.To, hint)
}

type UnsupportedColumnTypeError struct {
	t Type
}

func (e *UnsupportedColumnTypeError) Error() string {
	return fmt.Sprintf("clickhouse: unsupported column type %q", e.t)
}

type Interface interface {
	Type() Type
	Rows() int
	Row(i int, ptr bool) interface{}
	ScanRow(dest interface{}, row int) error
	Append(v interface{}) (nulls []uint8, err error)
	AppendRow(v interface{}) error
	Decode(decoder *binary.Decoder, rows int) error
	Encode(*binary.Encoder) error
	ScanType() reflect.Type
}

type CustomSerialization interface {
	ReadStatePrefix(*binary.Decoder) error
	WriteStatePrefix(*binary.Encoder) error
}
