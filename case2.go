package main

import (
	"reflect"
)

func GetTableName(e any) string {
	v := reflect.TypeOf(e)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	f := v.Field(0)
	val, ok := f.Tag.Lookup("table_name")
	if !ok {
		panic("Table name for entity not defined")
	}

	return val
}
