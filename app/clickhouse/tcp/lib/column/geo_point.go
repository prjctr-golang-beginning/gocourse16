package column

import (
	"fmt"
	"reflect"

	"github.com/paulmach/orb"
	"gocourse16/app/clickhouse/tcp/binary"
)

type Point struct {
	lon Float64
	lat Float64
}

func (col *Point) Type() Type {
	return "Point"
}

func (col *Point) ScanType() reflect.Type {
	return scanTypePoint
}

func (col *Point) Rows() int {
	return col.lon.Rows()
}

func (col *Point) Row(i int, ptr bool) interface{} {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *Point) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *orb.Point:
		*d = col.row(row)
	case **orb.Point:
		*d = new(orb.Point)
		**d = col.row(row)
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Point",
			Hint: fmt.Sprintf("try using *%s", col.ScanType()),
		}
	}
	return nil
}

func (col *Point) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []orb.Point:
		nulls = make([]uint8, len(v))
		for _, v := range v {
			col.lon = append(col.lon, v.Lon())
			col.lat = append(col.lat, v.Lat())
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Point",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}
func (col *Point) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case orb.Point:
		col.lon = append(col.lon, v.Lon())
		col.lat = append(col.lat, v.Lat())
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Point",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *Point) Decode(decoder *binary.Decoder, rows int) error {
	if err := col.lon.Decode(decoder, rows); err != nil {
		return err
	}
	if err := col.lat.Decode(decoder, rows); err != nil {
		return err
	}
	return nil
}

func (col *Point) Encode(encoder *binary.Encoder) error {
	if err := col.lon.Encode(encoder); err != nil {
		return err
	}
	return col.lat.Encode(encoder)
}

func (col *Point) row(i int) orb.Point {
	return orb.Point{
		col.lon[i],
		col.lat[i],
	}
}

var _ Interface = (*Point)(nil)