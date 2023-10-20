package datastreamer

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/ethereum/go-ethereum/common"
)

const (
	// StreamTypeSequencer represents a Sequencer stream
	StreamTypeSequencer StreamType = 1
	// EntryTypeL2BlockStart represents a L2 block start
	EntryTypeL2BlockStart EntryType = 1
	// EntryTypeL2Tx represents a L2 transaction
	EntryTypeL2Tx EntryType = 2
	// EntryTypeL2BlockEnd represents a L2 block end
	EntryTypeL2BlockEnd EntryType = 3
)

// EntityDefinition type to print entity events fields
type EntityDefinition struct {
	Name       string
	StreamType StreamType
	Definition reflect.Type
}

// ToString is only meant for testing purposes
func (e EntityDefinition) ToString(entity []byte) string {
	return e.toString(entity)
}

func (e EntityDefinition) toString(entity []byte) string {
	obj := reflect.NewAt(e.Definition, unsafe.Pointer(&entity[0]))
	val := obj.Elem()
	stringValue := ""
	accumulatedLength := 0
	for i := 0; i < val.NumField(); i++ {
		name := val.Type().Field(i).Name
		value := ""

		if val.Type().Field(i).Type.Kind() == reflect.Slice {
			value = "0x" + common.Bytes2Hex(entity[accumulatedLength:])
		} else if val.Type().Field(i).Type.Kind() == reflect.Uint8 {
			value = fmt.Sprintf("%v", entity[accumulatedLength])
		} else if val.Type().Field(i).Type.Kind() == reflect.Uint32 {
			value = fmt.Sprintf("%v", binary.LittleEndian.Uint32(entity[accumulatedLength:accumulatedLength+4]))
		} else if val.Type().Field(i).Type.Kind() == reflect.Uint64 {
			value = fmt.Sprintf("%v", binary.LittleEndian.Uint64(entity[accumulatedLength:accumulatedLength+8]))
		} else {
			value = fmt.Sprintf("%v", val.Field(i).Interface())
		}

		stringValue += fmt.Sprintf(name + ": " + value)
		if i < val.NumField()-1 {
			stringValue += ", "
		}

		accumulatedLength += int(val.Type().Field(i).Type.Size())
	}
	return stringValue
}
