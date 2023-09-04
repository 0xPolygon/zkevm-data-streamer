package db

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type L2Block struct {
	BatchNum  uint64
	BlockNum  uint64
	Timestamp time.Time
	StateRoot common.Hash
	Coinbase  common.Address
}

// Encode returns the encoded L2Block as a byte slice
func (L2Block) Encode() []byte {
	return []byte{}
}

type L2Transaction struct {
	BatchNum            uint64
	BlockNum            uint64
	EffectivePercentage uint8
	IsValid             uint8
	EncodedLength       uint32
	Encoded             []byte
}

// Encode returns the encoded L2Transaction as a byte slice
func (L2Transaction) Encode() []byte {
	return []byte{}
}
