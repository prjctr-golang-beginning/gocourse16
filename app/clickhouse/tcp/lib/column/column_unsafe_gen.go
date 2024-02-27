//go:build amd64 || arm64
// +build amd64 arm64

// Code generated by make codegen DO NOT EDIT.
// source: lib/column/codegen/column_safe.tpl

package column

import (
	"gocourse16/app/clickhouse/tcp/binary"
	"reflect"
	"unsafe"
)

func (col *Float32) Decode(decoder *binary.Decoder, rows int) error {
	if rows == 0 {
		return nil
	}
	const size = 32 / 8

	*col = append(*col, make([]float32, rows)...)

	var dst []byte
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
	slice.Len = len(*col) * size
	slice.Cap = cap(*col) * size

	if err := decoder.Raw(dst); err != nil {
		return err
	}
	return nil
}

func (col *Float32) Encode(encoder *binary.Encoder) error {
	if len(*col) == 0 {
		return nil
	}
	const size = 32 / 8
	scratch := make([]byte, size*len(*col))
	{
		var src []byte
		slice := (*reflect.SliceHeader)(unsafe.Pointer(&src))
		slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
		slice.Len = len(*col) * size
		slice.Cap = cap(*col) * size

		copy(scratch, src)
	}
	return encoder.Raw(scratch)
}

func (col *Float64) Decode(decoder *binary.Decoder, rows int) error {
	if rows == 0 {
		return nil
	}
	const size = 64 / 8

	*col = append(*col, make([]float64, rows)...)

	var dst []byte
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
	slice.Len = len(*col) * size
	slice.Cap = cap(*col) * size

	if err := decoder.Raw(dst); err != nil {
		return err
	}
	return nil
}

func (col *Float64) Encode(encoder *binary.Encoder) error {
	if len(*col) == 0 {
		return nil
	}
	const size = 64 / 8
	scratch := make([]byte, size*len(*col))
	{
		var src []byte
		slice := (*reflect.SliceHeader)(unsafe.Pointer(&src))
		slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
		slice.Len = len(*col) * size
		slice.Cap = cap(*col) * size

		copy(scratch, src)
	}
	return encoder.Raw(scratch)
}

func (col *Int8) Decode(decoder *binary.Decoder, rows int) error {
	if rows == 0 {
		return nil
	}
	const size = 8 / 8

	*col = append(*col, make([]int8, rows)...)

	var dst []byte
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
	slice.Len = len(*col) * size
	slice.Cap = cap(*col) * size

	if err := decoder.Raw(dst); err != nil {
		return err
	}
	return nil
}

func (col *Int8) Encode(encoder *binary.Encoder) error {
	if len(*col) == 0 {
		return nil
	}
	const size = 8 / 8
	scratch := make([]byte, size*len(*col))
	{
		var src []byte
		slice := (*reflect.SliceHeader)(unsafe.Pointer(&src))
		slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
		slice.Len = len(*col) * size
		slice.Cap = cap(*col) * size

		copy(scratch, src)
	}
	return encoder.Raw(scratch)
}

func (col *Int16) Decode(decoder *binary.Decoder, rows int) error {
	if rows == 0 {
		return nil
	}
	const size = 16 / 8

	*col = append(*col, make([]int16, rows)...)

	var dst []byte
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
	slice.Len = len(*col) * size
	slice.Cap = cap(*col) * size

	if err := decoder.Raw(dst); err != nil {
		return err
	}
	return nil
}

func (col *Int16) Encode(encoder *binary.Encoder) error {
	if len(*col) == 0 {
		return nil
	}
	const size = 16 / 8
	scratch := make([]byte, size*len(*col))
	{
		var src []byte
		slice := (*reflect.SliceHeader)(unsafe.Pointer(&src))
		slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
		slice.Len = len(*col) * size
		slice.Cap = cap(*col) * size

		copy(scratch, src)
	}
	return encoder.Raw(scratch)
}

func (col *Int32) Decode(decoder *binary.Decoder, rows int) error {
	if rows == 0 {
		return nil
	}
	const size = 32 / 8

	*col = append(*col, make([]int32, rows)...)

	var dst []byte
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
	slice.Len = len(*col) * size
	slice.Cap = cap(*col) * size

	if err := decoder.Raw(dst); err != nil {
		return err
	}
	return nil
}

func (col *Int32) Encode(encoder *binary.Encoder) error {
	if len(*col) == 0 {
		return nil
	}
	const size = 32 / 8
	scratch := make([]byte, size*len(*col))
	{
		var src []byte
		slice := (*reflect.SliceHeader)(unsafe.Pointer(&src))
		slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
		slice.Len = len(*col) * size
		slice.Cap = cap(*col) * size

		copy(scratch, src)
	}
	return encoder.Raw(scratch)
}

func (col *Int64) Decode(decoder *binary.Decoder, rows int) error {
	if rows == 0 {
		return nil
	}
	const size = 64 / 8

	*col = append(*col, make([]int64, rows)...)

	var dst []byte
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
	slice.Len = len(*col) * size
	slice.Cap = cap(*col) * size

	if err := decoder.Raw(dst); err != nil {
		return err
	}
	return nil
}

func (col *Int64) Encode(encoder *binary.Encoder) error {
	if len(*col) == 0 {
		return nil
	}
	const size = 64 / 8
	scratch := make([]byte, size*len(*col))
	{
		var src []byte
		slice := (*reflect.SliceHeader)(unsafe.Pointer(&src))
		slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
		slice.Len = len(*col) * size
		slice.Cap = cap(*col) * size

		copy(scratch, src)
	}
	return encoder.Raw(scratch)
}

func (col *UInt8) Decode(decoder *binary.Decoder, rows int) error {
	if rows == 0 {
		return nil
	}
	const size = 8 / 8

	*col = append(*col, make([]uint8, rows)...)

	var dst []byte
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
	slice.Len = len(*col) * size
	slice.Cap = cap(*col) * size

	if err := decoder.Raw(dst); err != nil {
		return err
	}
	return nil
}

func (col *UInt8) Encode(encoder *binary.Encoder) error {
	if len(*col) == 0 {
		return nil
	}
	const size = 8 / 8
	scratch := make([]byte, size*len(*col))
	{
		var src []byte
		slice := (*reflect.SliceHeader)(unsafe.Pointer(&src))
		slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
		slice.Len = len(*col) * size
		slice.Cap = cap(*col) * size

		copy(scratch, src)
	}
	return encoder.Raw(scratch)
}

func (col *UInt16) Decode(decoder *binary.Decoder, rows int) error {
	if rows == 0 {
		return nil
	}
	const size = 16 / 8

	*col = append(*col, make([]uint16, rows)...)

	var dst []byte
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
	slice.Len = len(*col) * size
	slice.Cap = cap(*col) * size

	if err := decoder.Raw(dst); err != nil {
		return err
	}
	return nil
}

func (col *UInt16) Encode(encoder *binary.Encoder) error {
	if len(*col) == 0 {
		return nil
	}
	const size = 16 / 8
	scratch := make([]byte, size*len(*col))
	{
		var src []byte
		slice := (*reflect.SliceHeader)(unsafe.Pointer(&src))
		slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
		slice.Len = len(*col) * size
		slice.Cap = cap(*col) * size

		copy(scratch, src)
	}
	return encoder.Raw(scratch)
}

func (col *UInt32) Decode(decoder *binary.Decoder, rows int) error {
	if rows == 0 {
		return nil
	}
	const size = 32 / 8

	*col = append(*col, make([]uint32, rows)...)

	var dst []byte
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
	slice.Len = len(*col) * size
	slice.Cap = cap(*col) * size

	if err := decoder.Raw(dst); err != nil {
		return err
	}
	return nil
}

func (col *UInt32) Encode(encoder *binary.Encoder) error {
	if len(*col) == 0 {
		return nil
	}
	const size = 32 / 8
	scratch := make([]byte, size*len(*col))
	{
		var src []byte
		slice := (*reflect.SliceHeader)(unsafe.Pointer(&src))
		slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
		slice.Len = len(*col) * size
		slice.Cap = cap(*col) * size

		copy(scratch, src)
	}
	return encoder.Raw(scratch)
}

func (col *UInt64) Decode(decoder *binary.Decoder, rows int) error {
	if rows == 0 {
		return nil
	}
	const size = 64 / 8

	*col = append(*col, make([]uint64, rows)...)

	var dst []byte
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
	slice.Len = len(*col) * size
	slice.Cap = cap(*col) * size

	if err := decoder.Raw(dst); err != nil {
		return err
	}
	return nil
}

func (col *UInt64) Encode(encoder *binary.Encoder) error {
	if len(*col) == 0 {
		return nil
	}
	const size = 64 / 8
	scratch := make([]byte, size*len(*col))
	{
		var src []byte
		slice := (*reflect.SliceHeader)(unsafe.Pointer(&src))
		slice.Data = (*reflect.SliceHeader)(unsafe.Pointer(col)).Data
		slice.Len = len(*col) * size
		slice.Cap = cap(*col) * size

		copy(scratch, src)
	}
	return encoder.Raw(scratch)
}
