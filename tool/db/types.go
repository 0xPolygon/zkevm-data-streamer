package db

import (
	"time"
	"unsafe"

	"github.com/ethereum/go-ethereum/common"
)

type L2Block struct {
	BatchNumber    uint64
	L2BlockNumber  uint64
	Timestamp      time.Time
	GlobalExitRoot common.Hash
	Coinbase       common.Address
}

// Encode returns the encoded L2Block as a byte slice
func (b L2Block) Encode() []byte {
	const size = int(unsafe.Sizeof(L2Block{}))
	return (*(*[size]byte)(unsafe.Pointer(&b)))[:]
}

type L2Transaction struct {
	BatchNumber                 uint64
	EffectiveGasPricePercentage uint8
	IsValid                     uint8
	EncodedLength               uint32
	Encoded                     []byte
}

// Encode returns the encoded L2Transaction as a byte slice
func (l L2Transaction) Encode() []byte {
	const size = int(unsafe.Sizeof(L2Transaction{}))
	return (*(*[size]byte)(unsafe.Pointer(&l)))[:]
}
