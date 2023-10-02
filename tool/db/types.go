package db

import (
	"encoding/binary"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/ethereum/go-ethereum/common"
)

const (
	// StreamTypeSequencer represents a Sequencer stream
	StreamTypeSequencer datastreamer.StreamType = 1
	// EntryTypeL2BlockStart represents a L2 block start
	EntryTypeL2BlockStart datastreamer.EntryType = 1
	// EntryTypeL2Tx represents a L2 transaction
	EntryTypeL2Tx datastreamer.EntryType = 2
	// EntryTypeL2BlockEnd represents a L2 block end
	EntryTypeL2BlockEnd datastreamer.EntryType = 3
)

// L2Block is a full l2 block
type L2Block struct {
	L2BlockStart
	L2BlockEnd
}

// L2BlockStart represents a L2 block start
type L2BlockStart struct {
	BatchNumber    uint64         // 8 bytes
	L2BlockNumber  uint64         // 8 bytes
	Timestamp      int64          // 8 bytes
	GlobalExitRoot common.Hash    // 32 bytes
	Coinbase       common.Address // 20 bytes
	ForkID         uint16         // 2 bytes
}

// Encode returns the encoded StartL2Block as a byte slice
func (b L2BlockStart) Encode() []byte {
	bytes := make([]byte, 0)
	bytes = binary.LittleEndian.AppendUint64(bytes, b.BatchNumber)
	bytes = binary.LittleEndian.AppendUint64(bytes, b.L2BlockNumber)
	bytes = binary.LittleEndian.AppendUint64(bytes, uint64(b.Timestamp))
	bytes = append(bytes, b.GlobalExitRoot.Bytes()...)
	bytes = append(bytes, b.Coinbase.Bytes()...)
	bytes = binary.LittleEndian.AppendUint16(bytes, b.ForkID)
	return bytes
}

// L2Transaction represents a L2 transaction
type L2Transaction struct {
	EffectiveGasPricePercentage uint8  // 1 byte
	IsValid                     uint8  // 1 byte
	EncodedLength               uint32 // 4 bytes
	Encoded                     []byte
}

// Encode returns the encoded L2Transaction as a byte slice
func (l L2Transaction) Encode() []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, byte(l.EffectiveGasPricePercentage))
	bytes = append(bytes, byte(l.IsValid))
	bytes = binary.LittleEndian.AppendUint32(bytes, l.EncodedLength)
	bytes = append(bytes, l.Encoded...)
	return bytes
}

// L2BlockEnd represents a L2 block end
type L2BlockEnd struct {
	BlockHash common.Hash // 32 bytes
	StateRoot common.Hash // 32 bytes
}

// Encode returns the encoded EndL2Block as a byte slice
func (b L2BlockEnd) Encode() []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, b.BlockHash.Bytes()...)
	bytes = append(bytes, b.StateRoot.Bytes()...)
	return bytes
}
