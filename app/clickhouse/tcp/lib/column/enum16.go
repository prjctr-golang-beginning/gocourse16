package column

import (
	"fmt"
	"reflect"

	"gocourse16/app/clickhouse/tcp/binary"
)

type Enum16 struct {
	iv     map[string]uint16
	vi     map[uint16]string
	chType Type
	values UInt16
}

func (e *Enum16) Type() Type {
	return e.chType
}

func (col *Enum16) ScanType() reflect.Type {
	return scanTypeString
}

func (e *Enum16) Rows() int {
	return len(e.values)
}

func (e *Enum16) Row(i int, ptr bool) interface{} {
	value := e.vi[e.values[i]]
	if ptr {
		return &value
	}
	return value
}

func (e *Enum16) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = e.vi[e.values[row]]
	case **string:
		*d = new(string)
		**d = e.vi[e.values[row]]
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Enum16",
		}
	}
	return nil
}

func (e *Enum16) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []string:
		nulls = make([]uint8, len(v))
		for _, elem := range v {
			v, ok := e.iv[elem]
			if !ok {
				return nil, &Error{
					Err:        fmt.Errorf("unknown element %q", elem),
					ColumnType: string(e.chType),
				}
			}
			e.values = append(e.values, v)
		}
	case []*string:
		nulls = make([]uint8, len(v))
		for i, elem := range v {
			switch {
			case elem != nil:
				v, ok := e.iv[*elem]
				if !ok {
					return nil, &Error{
						Err:        fmt.Errorf("unknown element %q", *elem),
						ColumnType: string(e.chType),
					}
				}
				e.values = append(e.values, v)
			default:
				e.values, nulls[i] = append(e.values, 0), 1
			}
		}
	}
	return
}

func (e *Enum16) AppendRow(elem interface{}) error {
	switch elem := elem.(type) {
	case string:
		v, ok := e.iv[elem]
		if !ok {
			return &Error{
				Err:        fmt.Errorf("unknown element %q", elem),
				ColumnType: string(e.chType),
			}
		}
		e.values = append(e.values, v)
	case *string:
		switch {
		case elem != nil:
			v, ok := e.iv[*elem]
			if !ok {
				return &Error{
					Err:        fmt.Errorf("unknown element %q", *elem),
					ColumnType: string(e.chType),
				}
			}
			e.values = append(e.values, v)
		default:
			e.values = append(e.values, 0)
		}
	case nil:
		e.values = append(e.values, 0)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Enum16",
			From: fmt.Sprintf("%T", elem),
		}
	}
	return nil
}

func (e *Enum16) Decode(decoder *binary.Decoder, rows int) error {
	return e.values.Decode(decoder, rows)
}

func (e *Enum16) Encode(encoder *binary.Encoder) error {
	return e.values.Encode(encoder)
}

var _ Interface = (*Enum16)(nil)
