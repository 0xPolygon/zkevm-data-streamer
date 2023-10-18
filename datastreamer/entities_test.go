package datastreamer_test

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

type testEntity struct {
	EffectiveGasPricePercentage uint8  // 1 byte
	IsValid                     uint8  // 1 byte
	EncodedLength               uint32 // 4 bytes
	AnotherField                uint32 // 4 bytes
	Encoded                     []byte // ? bytes
}

func (e testEntity) Encode() []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, byte(e.EffectiveGasPricePercentage))
	bytes = append(bytes, byte(e.IsValid))
	bytes = binary.LittleEndian.AppendUint32(bytes, e.EncodedLength)
	bytes = binary.LittleEndian.AppendUint32(bytes, e.AnotherField)
	bytes = append(bytes, e.Encoded...)
	return bytes
}

func TestEntityToString(t *testing.T) {
	l2Transaction := testEntity{
		EffectiveGasPricePercentage: 128,                   // 1 byte
		IsValid:                     1,                     // 1 byte
		EncodedLength:               5,                     // 4 bytes
		AnotherField:                10,                    // 4 bytes
		Encoded:                     []byte{1, 2, 3, 4, 5}, // 5 bytes
	}

	l2Transaction2 := testEntity{
		EffectiveGasPricePercentage: 255,                                   // 1 byte
		IsValid:                     1,                                     // 1 byte
		EncodedLength:               10,                                    // 4 bytes
		AnotherField:                22,                                    // 4 bytes
		Encoded:                     []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, // 10 bytes
	}

	entityDef := datastreamer.EntityDefinition{
		Name:       "L2Transaction",
		StreamType: datastreamer.StreamType(datastreamer.StreamTypeSequencer),
		Definition: reflect.TypeOf(testEntity{}),
	}

	stringValue := entityDef.ToString(l2Transaction.Encode())
	expected := fmt.Sprintf("EffectiveGasPricePercentage: %d, IsValid: %d, EncodedLength: %d, AnotherField: %d, Encoded: %s", l2Transaction.EffectiveGasPricePercentage, l2Transaction.IsValid, l2Transaction.EncodedLength, l2Transaction.AnotherField, "0x"+common.Bytes2Hex(l2Transaction.Encoded))
	assert.Equal(t, expected, stringValue)

	stringValue = entityDef.ToString(l2Transaction2.Encode())
	expected = fmt.Sprintf("EffectiveGasPricePercentage: %d, IsValid: %d, EncodedLength: %d, AnotherField: %d, Encoded: %s", l2Transaction2.EffectiveGasPricePercentage, l2Transaction2.IsValid, l2Transaction2.EncodedLength, l2Transaction2.AnotherField, "0x"+common.Bytes2Hex(l2Transaction2.Encoded))
	assert.Equal(t, expected, stringValue)
}
