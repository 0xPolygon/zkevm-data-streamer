package db

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

func TestL2BlockEncode(t *testing.T) {
	l2Block := L2BlockStart{
		BatchNumber:    1,                           // 8 bytes
		L2BlockNumber:  2,                           // 8 bytes
		Timestamp:      3,                           // 8 bytes
		GlobalExitRoot: common.HexToHash("0x04"),    // 32 bytes
		Coinbase:       common.HexToAddress("0x05"), // 20 bytes
		ForkID:         5,
	}

	encoded := l2Block.Encode()
	expected := []byte{1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 5, 0}

	assert.Equal(t, expected, encoded)
}

func TestL2TransactionEncode(t *testing.T) {
	l2Transaction := L2Transaction{
		EffectiveGasPricePercentage: 128,                   // 1 byte
		IsValid:                     1,                     // 1 byte
		EncodedLength:               5,                     // 4 bytes
		Encoded:                     []byte{1, 2, 3, 4, 5}, // 5 bytes
	}

	encoded := l2Transaction.Encode()
	expected := []byte{128, 1, 5, 0, 0, 0, 1, 2, 3, 4, 5}
	assert.Equal(t, expected, encoded)
}
