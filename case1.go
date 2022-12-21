package main

import (
	"fmt"
	"reflect"
	"strings"
)

type FieldsCache map[string][]string

func (c FieldsCache) GetFields(e any, t string) ([]string, error) {
	var err error
	var fs []string
	var ok bool

	if fs, ok = c[t]; !ok {
		fs, err = c.cacheFields(e, t)
	}

	return fs, err
}

func (c *FieldsCache) addFields(t string, fs []string) {
	fc := *c
	fc[t] = fs
}

func (c *FieldsCache) cacheFields(e any, t string) ([]string, error) {
	rt := reflect.TypeOf(e)
	if rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("bad type")
	}
	var res []string
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		v := strings.Split(f.Tag.Get(`json`), ",")[0] // use split to ignore tag "options" like omitempty, etc.
		//if !strings.HasPrefix(v, `__`) {              // skip fields with metadata
		res = append(res, v)
		//}
	}

	c.addFields(t, res)

	return res, nil
}
