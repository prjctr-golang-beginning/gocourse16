package main

import (
	"fmt"
	"reflect"
	"time"
)

type Differ interface {
	GetDiff() []string
}

func main() {
	t := TrackTimeBegin()
	defer func() { fmt.Printf("Time pased: %d", t.Passed()) }()
	defer Recover()

	entity := &DTONewEan{}
	// case 1
	fc := FieldsCache{}
	fs, err := fc.GetFields(entity, `ean`)
	if err != nil {
		panic(err)
	}
	fmt.Println(`1. Cached fields:`, fs)

	// case 2
	var iEntity Differ = entity
	if reflect.ValueOf(iEntity).IsNil() {
		fmt.Println("2. Entity is nil")
	} else {
		fmt.Println("2. Entity doesn't nil")
	}

	// case 3
	if reflect.DeepEqual(entity, &DTONewEan{ArtId: 100}) {
		fmt.Println("3. Entities don't equal")
	} else {
		fmt.Println("3. Entities are equal")
	}

	// case 4
	tName := GetTableName(entity)
	fmt.Println(`4. Table name:`, tName)

	// case 5
	fmt.Printf("5. Entity before: %v\n", entity)
	Populate(entity)
	fmt.Printf("5. Entity after: %v\n", entity)
}

type TimeTracker int64

func TrackTimeBegin() TimeTracker {
	return TimeTracker(time.Now().UnixNano())
}

func (t TimeTracker) Passed() int64 {
	return time.Now().UnixNano() - int64(t)
}

func Recover() {
	if r := recover(); r != nil {
		newErr := fmt.Errorf("recovered with message: '%s', stack trace:\r\n", r)
		fmt.Println(newErr)
	} else {
		fmt.Println(`Panics not found`)
	}
}
