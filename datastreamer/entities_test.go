package datastreamer_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/stretchr/testify/assert"
)

func TestEntityToString(t *testing.T) {
	l2Transaction := datastreamer.L2Transaction{
		EffectiveGasPricePercentage: 128,                   // 1 byte
		IsValid:                     1,                     // 1 byte
		EncodedLength:               5,                     // 4 bytes
		Encoded:                     []byte{1, 2, 3, 4, 5}, // 5 bytes
	}

	l2Transaction2 := datastreamer.L2Transaction{
		EffectiveGasPricePercentage: 255,                                   // 1 byte
		IsValid:                     1,                                     // 1 byte
		EncodedLength:               10,                                    // 4 bytes
		Encoded:                     []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, // 10bytes
	}

	entityDef := datastreamer.EntityDefinition{
		Name:       "L2Transaction",
		StreamType: datastreamer.StreamType(datastreamer.StreamTypeSequencer),
		Definition: reflect.TypeOf(datastreamer.L2Transaction{}),
	}

	stringValue := entityDef.ToString(l2Transaction.Encode())
	expected := fmt.Sprintf("EffectiveGasPricePercentage: %d, IsValid: %d, EncodedLength: %d, Encoded: %s", l2Transaction.EffectiveGasPricePercentage, l2Transaction.IsValid, l2Transaction.EncodedLength, l2Transaction.Encoded)
	assert.Equal(t, expected, stringValue)

	stringValue = entityDef.ToString(l2Transaction2.Encode())
	expected = fmt.Sprintf("EffectiveGasPricePercentage: %d, IsValid: %d, EncodedLength: %d, Encoded: %s", l2Transaction2.EffectiveGasPricePercentage, l2Transaction2.IsValid, l2Transaction2.EncodedLength, l2Transaction2.Encoded)
	assert.Equal(t, expected, stringValue)
}
