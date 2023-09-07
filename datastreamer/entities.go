package datastreamer

import (
	"fmt"
	"reflect"
	"unsafe"
)

type EntityDefinition struct {
	Name       string
	StreamType StreamType
	Definition reflect.Type
}

func (e EntityDefinition) toString(entity []byte) string {
	obj := reflect.NewAt(e.Definition, unsafe.Pointer(&entity[0]))
	val := obj.Elem()
	stringValue := ""

	for i := 0; i < val.NumField(); i++ {
		stringValue += fmt.Sprintf(val.Type().Field(i).Name + ": " + fmt.Sprintf("%v", val.Field(i).Interface()))
		if i < val.NumField()-1 {
			stringValue += ", "
		}
	}
	return stringValue
}
