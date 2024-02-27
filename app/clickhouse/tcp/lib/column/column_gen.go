package column

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/paulmach/orb"
	"github.com/shopspring/decimal"
	"math/big"
	"net"
	"reflect"
	"strings"
	"time"
)

func (t Type) Column() (Interface, error) {
	switch t {
	case "Float32":
		return &Float32{}, nil
	case "Float64":
		return &Float64{}, nil
	case "Int8":
		return &Int8{}, nil
	case "Int16":
		return &Int16{}, nil
	case "Int32":
		return &Int32{}, nil
	case "Int64":
		return &Int64{}, nil
	case "UInt8":
		return &UInt8{}, nil
	case "UInt16":
		return &UInt16{}, nil
	case "UInt32":
		return &UInt32{}, nil
	case "UInt64":
		return &UInt64{}, nil
	case "Int128":
		return &BigInt{
			size:   16,
			chType: t,
		}, nil
	case "Int256":
		return &BigInt{
			size:   32,
			chType: t,
		}, nil
	case "UInt256":
		return &BigInt{
			size:   32,
			chType: t,
		}, nil
	case "IPv4":
		return &IPv4{}, nil
	case "IPv6":
		return &IPv6{}, nil
	case "Bool", "Boolean":
		return &Bool{}, nil
	case "Date":
		return &Date{}, nil
	case "Date32":
		return &Date32{}, nil
	case "UUID":
		return &UUID{}, nil
	case "Nothing":
		return &Nothing{}, nil
	case "Ring":
		v, err := (&Array{}).parse("Array(Point)")
		if err != nil {
			return nil, err
		}
		set := v.(*Array)
		set.chType = "Ring"
		return &Ring{
			set: set,
		}, nil
	case "Polygon":
		v, err := (&Array{}).parse("Array(Ring)")
		if err != nil {
			return nil, err
		}
		set := v.(*Array)
		set.chType = "Polygon"
		return &Polygon{
			set: set,
		}, nil
	case "MultiPolygon":
		v, err := (&Array{}).parse("Array(Polygon)")
		if err != nil {
			return nil, err
		}
		set := v.(*Array)
		set.chType = "MultiPolygon"
		return &MultiPolygon{
			set: set,
		}, nil
	case "Point":
		return &Point{}, nil
	case "String":
		return &String{}, nil
	}

	switch strType := string(t); {
	case strings.HasPrefix(string(t), "Map("):
		return (&Map{}).parse(t)
	case strings.HasPrefix(string(t), "Tuple("):
		return (&Tuple{}).parse(t)
	case strings.HasPrefix(string(t), "Decimal("):
		return (&Decimal{}).parse(t)
	case strings.HasPrefix(strType, "Nested("):
		return (&Nested{}).parse(t)
	case strings.HasPrefix(string(t), "Array("):
		return (&Array{}).parse(t)
	case strings.HasPrefix(string(t), "Interval"):
		return (&Interval{}).parse(t)
	case strings.HasPrefix(string(t), "Nullable"):
		return (&Nullable{}).parse(t)
	case strings.HasPrefix(string(t), "FixedString"):
		return (&FixedString{}).parse(t)
	case strings.HasPrefix(string(t), "LowCardinality"):
		return (&LowCardinality{}).parse(t)
	case strings.HasPrefix(string(t), "SimpleAggregateFunction"):
		return (&SimpleAggregateFunction{}).parse(t)
	case strings.HasPrefix(string(t), "Enum8") || strings.HasPrefix(string(t), "Enum16"):
		return Enum(t)
	case strings.HasPrefix(string(t), "DateTime64"):
		return (&DateTime64{}).parse(t)
	case strings.HasPrefix(strType, "DateTime") && !strings.HasPrefix(strType, "DateTime64"):
		return (&DateTime{}).parse(t)
	}
	return nil, &UnsupportedColumnTypeError{
		t: t,
	}
}

type (
	Float32 []float32
	Float64 []float64
	Int8    []int8
	Int16   []int16
	Int32   []int32
	Int64   []int64
	UInt8   []uint8
	UInt16  []uint16
	UInt32  []uint32
	UInt64  []uint64
)

var (
	_ Interface = (*Float32)(nil)
	_ Interface = (*Float64)(nil)
	_ Interface = (*Int8)(nil)
	_ Interface = (*Int16)(nil)
	_ Interface = (*Int32)(nil)
	_ Interface = (*Int64)(nil)
	_ Interface = (*UInt8)(nil)
	_ Interface = (*UInt16)(nil)
	_ Interface = (*UInt32)(nil)
	_ Interface = (*UInt64)(nil)
)

var (
	scanTypeFloat32      = reflect.TypeOf(float32(0))
	scanTypeFloat64      = reflect.TypeOf(float64(0))
	scanTypeInt8         = reflect.TypeOf(int8(0))
	scanTypeInt16        = reflect.TypeOf(int16(0))
	scanTypeInt32        = reflect.TypeOf(int32(0))
	scanTypeInt64        = reflect.TypeOf(int64(0))
	scanTypeUInt8        = reflect.TypeOf(uint8(0))
	scanTypeUInt16       = reflect.TypeOf(uint16(0))
	scanTypeUInt32       = reflect.TypeOf(uint32(0))
	scanTypeUInt64       = reflect.TypeOf(uint64(0))
	scanTypeIP           = reflect.TypeOf(net.IP{})
	scanTypeBool         = reflect.TypeOf(true)
	scanTypeByte         = reflect.TypeOf([]byte{})
	scanTypeUUID         = reflect.TypeOf(uuid.UUID{})
	scanTypeTime         = reflect.TypeOf(time.Time{})
	scanTypeRing         = reflect.TypeOf(orb.Ring{})
	scanTypePoint        = reflect.TypeOf(orb.Point{})
	scanTypeSlice        = reflect.TypeOf([]interface{}{})
	scanTypeBigInt       = reflect.TypeOf(&big.Int{})
	scanTypeString       = reflect.TypeOf("")
	scanTypePolygon      = reflect.TypeOf(orb.Polygon{})
	scanTypeDecimal      = reflect.TypeOf(decimal.Decimal{})
	scanTypeMultiPolygon = reflect.TypeOf(orb.MultiPolygon{})
)

func (col *Float32) Type() Type {
	return "Float32"
}

func (col *Float32) ScanType() reflect.Type {
	return scanTypeFloat32
}

func (col *Float32) Rows() int {
	return len(*col)
}

func (col *Float32) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *float32:
		*d = value[row]
	case **float32:
		*d = new(float32)
		**d = value[row]
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Float32",
			Hint: fmt.Sprintf("try using *%s", scanTypeFloat32),
		}
	}
	return nil
}

func (col *Float32) Row(i int, ptr bool) interface{} {
	value := *col
	if ptr {
		return &value[i]
	}
	return value[i]
}

func (col *Float32) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []float32:
		*col, nulls = append(*col, v...), make([]uint8, len(v))
	case []*float32:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				*col = append(*col, *v)
			default:
				*col, nulls[i] = append(*col, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Float32",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Float32) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case float32:
		*col = append(*col, v)
	case *float32:
		switch {
		case v != nil:
			*col = append(*col, *v)
		default:
			*col = append(*col, 0)
		}
	case nil:
		*col = append(*col, 0)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Float32",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *Float64) Type() Type {
	return "Float64"
}

func (col *Float64) ScanType() reflect.Type {
	return scanTypeFloat64
}

func (col *Float64) Rows() int {
	return len(*col)
}

func (col *Float64) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *float64:
		*d = value[row]
	case **float64:
		*d = new(float64)
		**d = value[row]
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Float64",
			Hint: fmt.Sprintf("try using *%s", scanTypeFloat64),
		}
	}
	return nil
}

func (col *Float64) Row(i int, ptr bool) interface{} {
	value := *col
	if ptr {
		return &value[i]
	}
	return value[i]
}

func (col *Float64) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []float64:
		*col, nulls = append(*col, v...), make([]uint8, len(v))
	case []*float64:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				*col = append(*col, *v)
			default:
				*col, nulls[i] = append(*col, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Float64",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Float64) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case float64:
		*col = append(*col, v)
	case *float64:
		switch {
		case v != nil:
			*col = append(*col, *v)
		default:
			*col = append(*col, 0)
		}
	case nil:
		*col = append(*col, 0)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Float64",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *Int8) Type() Type {
	return "Int8"
}

func (col *Int8) ScanType() reflect.Type {
	return scanTypeInt8
}

func (col *Int8) Rows() int {
	return len(*col)
}

func (col *Int8) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *int8:
		*d = value[row]
	case **int8:
		*d = new(int8)
		**d = value[row]
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Int8",
			Hint: fmt.Sprintf("try using *%s", scanTypeInt8),
		}
	}
	return nil
}

func (col *Int8) Row(i int, ptr bool) interface{} {
	value := *col
	if ptr {
		return &value[i]
	}
	return value[i]
}

func (col *Int8) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []int8:
		*col, nulls = append(*col, v...), make([]uint8, len(v))
	case []*int8:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				*col = append(*col, *v)
			default:
				*col, nulls[i] = append(*col, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Int8",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Int8) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case int8:
		*col = append(*col, v)
	case *int8:
		switch {
		case v != nil:
			*col = append(*col, *v)
		default:
			*col = append(*col, 0)
		}
	case nil:
		*col = append(*col, 0)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Int8",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *Int16) Type() Type {
	return "Int16"
}

func (col *Int16) ScanType() reflect.Type {
	return scanTypeInt16
}

func (col *Int16) Rows() int {
	return len(*col)
}

func (col *Int16) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *int16:
		*d = value[row]
	case **int16:
		*d = new(int16)
		**d = value[row]
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Int16",
			Hint: fmt.Sprintf("try using *%s", scanTypeInt16),
		}
	}
	return nil
}

func (col *Int16) Row(i int, ptr bool) interface{} {
	value := *col
	if ptr {
		return &value[i]
	}
	return value[i]
}

func (col *Int16) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []int16:
		*col, nulls = append(*col, v...), make([]uint8, len(v))
	case []*int16:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				*col = append(*col, *v)
			default:
				*col, nulls[i] = append(*col, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Int16",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Int16) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case int16:
		*col = append(*col, v)
	case *int16:
		switch {
		case v != nil:
			*col = append(*col, *v)
		default:
			*col = append(*col, 0)
		}
	case nil:
		*col = append(*col, 0)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Int16",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *Int32) Type() Type {
	return "Int32"
}

func (col *Int32) ScanType() reflect.Type {
	return scanTypeInt32
}

func (col *Int32) Rows() int {
	return len(*col)
}

func (col *Int32) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *int32:
		*d = value[row]
	case **int32:
		*d = new(int32)
		**d = value[row]
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Int32",
			Hint: fmt.Sprintf("try using *%s", scanTypeInt32),
		}
	}
	return nil
}

func (col *Int32) Row(i int, ptr bool) interface{} {
	value := *col
	if ptr {
		return &value[i]
	}
	return value[i]
}

func (col *Int32) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []int32:
		*col, nulls = append(*col, v...), make([]uint8, len(v))
	case []*int32:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				*col = append(*col, *v)
			default:
				*col, nulls[i] = append(*col, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Int32",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Int32) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case int32:
		*col = append(*col, v)
	case *int32:
		switch {
		case v != nil:
			*col = append(*col, *v)
		default:
			*col = append(*col, 0)
		}
	case nil:
		*col = append(*col, 0)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Int32",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *Int64) Type() Type {
	return "Int64"
}

func (col *Int64) ScanType() reflect.Type {
	return scanTypeInt64
}

func (col *Int64) Rows() int {
	return len(*col)
}

func (col *Int64) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *int64:
		*d = value[row]
	case **int64:
		*d = new(int64)
		**d = value[row]
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Int64",
			Hint: fmt.Sprintf("try using *%s", scanTypeInt64),
		}
	}
	return nil
}

func (col *Int64) Row(i int, ptr bool) interface{} {
	value := *col
	if ptr {
		return &value[i]
	}
	return value[i]
}

func (col *Int64) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []int64:
		*col, nulls = append(*col, v...), make([]uint8, len(v))
	case []*int64:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				*col = append(*col, *v)
			default:
				*col, nulls[i] = append(*col, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Int64",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Int64) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case int64:
		*col = append(*col, v)
	case *int64:
		switch {
		case v != nil:
			*col = append(*col, *v)
		default:
			*col = append(*col, 0)
		}
	case nil:
		*col = append(*col, 0)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Int64",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *UInt8) Type() Type {
	return "UInt8"
}

func (col *UInt8) ScanType() reflect.Type {
	return scanTypeUInt8
}

func (col *UInt8) Rows() int {
	return len(*col)
}

func (col *UInt8) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *uint8:
		*d = value[row]
	case **uint8:
		*d = new(uint8)
		**d = value[row]
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "UInt8",
			Hint: fmt.Sprintf("try using *%s", scanTypeUInt8),
		}
	}
	return nil
}

func (col *UInt8) Row(i int, ptr bool) interface{} {
	value := *col
	if ptr {
		return &value[i]
	}
	return value[i]
}

func (col *UInt8) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []uint8:
		*col, nulls = append(*col, v...), make([]uint8, len(v))
	case []*uint8:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				*col = append(*col, *v)
			default:
				*col, nulls[i] = append(*col, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "UInt8",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *UInt8) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case uint8:
		*col = append(*col, v)
	case *uint8:
		switch {
		case v != nil:
			*col = append(*col, *v)
		default:
			*col = append(*col, 0)
		}
	case nil:
		*col = append(*col, 0)
	case bool:
		var t uint8
		if v {
			t = 1
		}
		*col = append(*col, t)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "UInt8",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *UInt16) Type() Type {
	return "UInt16"
}

func (col *UInt16) ScanType() reflect.Type {
	return scanTypeUInt16
}

func (col *UInt16) Rows() int {
	return len(*col)
}

func (col *UInt16) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *uint16:
		*d = value[row]
	case **uint16:
		*d = new(uint16)
		**d = value[row]
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "UInt16",
			Hint: fmt.Sprintf("try using *%s", scanTypeUInt16),
		}
	}
	return nil
}

func (col *UInt16) Row(i int, ptr bool) interface{} {
	value := *col
	if ptr {
		return &value[i]
	}
	return value[i]
}

func (col *UInt16) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []uint16:
		*col, nulls = append(*col, v...), make([]uint8, len(v))
	case []*uint16:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				*col = append(*col, *v)
			default:
				*col, nulls[i] = append(*col, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "UInt16",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *UInt16) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case uint16:
		*col = append(*col, v)
	case *uint16:
		switch {
		case v != nil:
			*col = append(*col, *v)
		default:
			*col = append(*col, 0)
		}
	case nil:
		*col = append(*col, 0)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "UInt16",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *UInt32) Type() Type {
	return "UInt32"
}

func (col *UInt32) ScanType() reflect.Type {
	return scanTypeUInt32
}

func (col *UInt32) Rows() int {
	return len(*col)
}

func (col *UInt32) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *uint32:
		*d = value[row]
	case **uint32:
		*d = new(uint32)
		**d = value[row]
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "UInt32",
			Hint: fmt.Sprintf("try using *%s", scanTypeUInt32),
		}
	}
	return nil
}

func (col *UInt32) Row(i int, ptr bool) interface{} {
	value := *col
	if ptr {
		return &value[i]
	}
	return value[i]
}

func (col *UInt32) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []uint32:
		*col, nulls = append(*col, v...), make([]uint8, len(v))
	case []*uint32:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				*col = append(*col, *v)
			default:
				*col, nulls[i] = append(*col, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "UInt32",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *UInt32) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case uint32:
		*col = append(*col, v)
	case *uint32:
		switch {
		case v != nil:
			*col = append(*col, *v)
		default:
			*col = append(*col, 0)
		}
	case nil:
		*col = append(*col, 0)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "UInt32",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *UInt64) Type() Type {
	return "UInt64"
}

func (col *UInt64) ScanType() reflect.Type {
	return scanTypeUInt64
}

func (col *UInt64) Rows() int {
	return len(*col)
}

func (col *UInt64) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *uint64:
		*d = value[row]
	case **uint64:
		*d = new(uint64)
		**d = value[row]
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "UInt64",
			Hint: fmt.Sprintf("try using *%s", scanTypeUInt64),
		}
	}
	return nil
}

func (col *UInt64) Row(i int, ptr bool) interface{} {
	value := *col
	if ptr {
		return &value[i]
	}
	return value[i]
}

func (col *UInt64) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []uint64:
		*col, nulls = append(*col, v...), make([]uint8, len(v))
	case []*uint64:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				*col = append(*col, *v)
			default:
				*col, nulls[i] = append(*col, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "UInt64",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *UInt64) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case uint64:
		*col = append(*col, v)
	case *uint64:
		switch {
		case v != nil:
			*col = append(*col, *v)
		default:
			*col = append(*col, 0)
		}
	case nil:
		*col = append(*col, 0)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "UInt64",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}
