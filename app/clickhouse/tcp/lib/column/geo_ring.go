package column

import (
	"fmt"
	"reflect"

	"github.com/paulmach/orb"
	"gocourse16/app/clickhouse/tcp/binary"
)

type Ring struct {
	set *Array
}

func (col *Ring) Type() Type {
	return "Ring"
}

func (col *Ring) ScanType() reflect.Type {
	return scanTypeRing
}

func (col *Ring) Rows() int {
	return col.set.Rows()
}

func (col *Ring) Row(i int, ptr bool) interface{} {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *Ring) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *orb.Ring:
		*d = col.row(row)
	case **orb.Ring:
		*d = new(orb.Ring)
		**d = col.row(row)
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Ring",
			Hint: fmt.Sprintf("try using *%s", col.ScanType()),
		}
	}
	return nil
}

func (col *Ring) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []orb.Ring:
		values := make([][]orb.Point, 0, len(v))
		for _, v := range v {
			values = append(values, v)
		}
		return col.set.Append(values)

	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Ring",
			From: fmt.Sprintf("%T", v),
		}
	}
}

func (col *Ring) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case orb.Ring:
		return col.set.AppendRow([]orb.Point(v))
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Ring",
			From: fmt.Sprintf("%T", v),
		}
	}
}

func (col *Ring) Decode(decoder *binary.Decoder, rows int) error {
	return col.set.Decode(decoder, rows)
}

func (col *Ring) Encode(encoder *binary.Encoder) error {
	return col.set.Encode(encoder)
}

func (col *Ring) row(i int) orb.Ring {
	var value []orb.Point
	{
		col.set.ScanRow(&value, i)
	}
	return value
}

var _ Interface = (*Ring)(nil)
