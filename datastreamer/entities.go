package datastreamer

import (
	"fmt"
	"reflect"
	"unsafe"
)

// EntityDefinition type to print entity events fields
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
		name := val.Type().Field(i).Name
		value := ""

		if val.Type().Field(i).Type.Kind() == reflect.Slice {
			value = "[slice]"
		} else {
			value = fmt.Sprintf("%v", val.Field(i).Interface())
		}

		stringValue += fmt.Sprintf(name + ": " + value)
		if i < val.NumField()-1 {
			stringValue += ", "
		}
	}
	return stringValue
}
