package datastreamer

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
)

type EntityDefinition struct {
	Name       string
	StreamType StreamType
	Definition reflect.Type
}

func (e EntityDefinition) toString(entity []byte) string {
	var bigEndian = (*(*[2]uint8)(unsafe.Pointer(&[]uint16{1}[0])))[0] == 0
	obj := reflect.NewAt(e.Definition, unsafe.Pointer(&entity[0]))
	val := obj.Elem()
	stringValue := ""
	accumulatedSize := 0

	for i := 0; i < val.NumField(); i++ {
		name := val.Type().Field(i).Name
		value := ""

		if val.Type().Field(i).Type.Kind() == reflect.Uint64 {
			if bigEndian {
				value = fmt.Sprintf("%v", binary.BigEndian.Uint64(entity))
			} else {
				value = fmt.Sprintf("%v", binary.LittleEndian.Uint64(entity))
			}
			accumulatedSize += 8
		} else if val.Type().Field(i).Type.Kind() == reflect.Uint32 {
			if bigEndian {
				value = fmt.Sprintf("%v", binary.BigEndian.Uint32(entity))
			} else {
				value = fmt.Sprintf("%v", binary.LittleEndian.Uint32(entity))
			}
			accumulatedSize += 4
		} else if val.Type().Field(i).Type.Kind() == reflect.Uint8 {
			value = fmt.Sprintf("%v", entity[0])
			accumulatedSize += 1
		} else if val.Type().Field(i).Type.Kind() == reflect.Slice {
			if len(entity) > accumulatedSize {
				value = fmt.Sprintf("%v", entity[accumulatedSize:len(entity)-accumulatedSize])
			} else {
				value = "[slice]"
			}
		} else {
			value = fmt.Sprintf("%v", val.Field(i).Interface())
		}

		stringValue += fmt.Sprintf(name + ": " + value)
		if i < val.NumField()-1 {
			stringValue += ", "
		}

		log.Debug(stringValue)
	}
	return stringValue
}
