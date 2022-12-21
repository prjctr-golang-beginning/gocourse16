package main

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func Populate(ent any) {
	var err error

	fc := FieldsCache{}
	v := reflect.Indirect(reflect.ValueOf(ent).Elem())
	tags, err := fc.GetFields(ent, "entity")
	if err != nil {
		panic(err)
	}

	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if f.Kind() == reflect.Ptr {
			f = f.Elem()
		}
		mf := tags[i]
		val := GertFromContainerByFieldName(mf)
		if val == nil {
			continue
		}

		if f.IsValid() {
			switch f.Kind() {
			case reflect.String:
				if mf == "goods_images_emails_notify" { // crutch for Merchant entity
					gienValStr := ""
					for _, gienVal := range val.([]any) {
						gienValStr += " " + fmt.Sprintf("%s", gienVal)
					}
					f.SetString(strings.Trim(gienValStr, " "))
				} else {
					switch tVal := val.(type) {
					case float64:
						f.SetString(fmt.Sprintf("%f", tVal))
					case bool:
						if tVal {
							f.SetString("true")
						} else {
							f.SetString("false")
						}
					default:
						f.SetString(val.(string))
					}
				}
			case reflect.Int:
				switch val.(type) {
				case string:
					value, _ := strconv.Atoi(val.(string))
					f.SetInt(int64(value))
				default:
					if tmp, ok := val.(float64); ok {
						f.SetInt(int64(tmp))
					}
					if tmp, ok := val.(int64); ok {
						f.SetInt(tmp)
					}
					if tmp, ok := val.(bool); ok {
						if tmp {
							f.SetInt(1)
						} else {
							f.SetInt(0)
						}
					}
				}
			case reflect.Float64:
				switch val.(type) {
				case string:
					value := 0.0
					if val != "" {
						value, _ = strconv.ParseFloat(val.(string), 64)
					}
					f.SetFloat(value)
				default:
					f.SetFloat(val.(float64))
				}
			case reflect.Bool:
				switch val.(type) {
				case string:
					value, _ := strconv.Atoi(val.(string))
					f.SetBool(value != 0)
				case float64:
					f.SetBool(val.(float64) != 0)
				default:
					f.SetBool(val.(bool))
				}
			case reflect.Struct:
				if val.(string) != "" && val.(string) != "0000-00-00 00:00:00" {
					var t time.Time
					loc := &time.Location{}
					t, err = time.ParseInLocation("2006-01-02 15:04:05", val.(string), loc)
					if err != nil {
						fmt.Printf("Datetime don't parsed correctly for field \"%s\", err is %v\n", mf, err)
					}

					f.Set(reflect.ValueOf(t))
				}
			case reflect.Slice:
				fType := f.Type()
				if fType.String() == "[]string" {
					switch reflect.TypeOf(val).Kind() {
					case reflect.Slice:
						v := reflect.ValueOf(val)
						b := reflect.MakeSlice(fType, 0, v.Cap())

						for i := 0; i < v.Len(); i++ {
							s := fmt.Sprint(v.Index(i))
							b = reflect.Append(b, reflect.ValueOf(s))
						}

						f.Set(b)
					}
				}
			case reflect.Map:
				fType := f.Type()
				if fType.String() == "map[string]datatype.FormattedFloat64" {
					switch reflect.TypeOf(val).Kind() {
					case reflect.Map:
						v := reflect.ValueOf(val)

						if v.Type().String() == "map[string]interface {}" {
							b := reflect.MakeMap(fType)

							for _, mK := range v.MapKeys() {
								mV := v.MapIndex(mK)
								switch rV := mV.Interface().(type) {
								case float64:
									b.SetMapIndex(mK, reflect.ValueOf(rV))
								}
							}

							f.Set(b)
						}
					}
				}
			default:
				fmt.Printf("Unpopulated field %s of type %s\n", mf, f.Kind())
			}
		}
	}
}

func GertFromContainerByFieldName(fn string) any {
	fv := GetFieldsValues()

	return fv[fn]
}
