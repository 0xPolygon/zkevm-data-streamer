package db

import (
	"encoding/binary"
	"unsafe"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/ethereum/go-ethereum/common"
)

const (
	// StreamTypeSequencer represents a Sequencer stream
	StreamTypeSequencer datastreamer.StreamType = 1
	// EntryTypeL2Block represents a L2 block
	EntryTypeL2Block datastreamer.EntryType = 1
	// EntryTypeL2Tx represents a L2 transaction
	EntryTypeL2Tx datastreamer.EntryType = 2
)

// L2Block represents a L2 block
type L2Block struct {
	BatchNumber    uint64         // 8 bytes
	L2BlockNumber  uint64         // 8 bytes
	Timestamp      int64          // 8 bytes
	GlobalExitRoot common.Hash    // 32 bytes
	Coinbase       common.Address // 20 bytes
}

// Encode returns the encoded L2Block as a byte slice
func (b L2Block) Encode() []byte {
	log.Debugf("BatchNumber: %v", b.BatchNumber)
	var bigEndian = (*(*[2]uint8)(unsafe.Pointer(&[]uint16{1}[0])))[0] == 0

	bytes := make([]byte, 0)
	if bigEndian {
		bytes = binary.BigEndian.AppendUint64(bytes, b.BatchNumber)
		bytes = binary.BigEndian.AppendUint64(bytes, b.L2BlockNumber)
		bytes = binary.BigEndian.AppendUint64(bytes, uint64(b.Timestamp))
	} else {
		bytes = binary.LittleEndian.AppendUint64(bytes, b.BatchNumber)
		bytes = binary.LittleEndian.AppendUint64(bytes, b.L2BlockNumber)
		bytes = binary.LittleEndian.AppendUint64(bytes, uint64(b.Timestamp))
	}
	bytes = append(bytes, b.GlobalExitRoot.Bytes()...)
	bytes = append(bytes, b.Coinbase.Bytes()...)

	log.Debugf("Encoded L2Block: %v", bytes)

	return bytes
}

// L2Transaction represents a L2 transaction
type L2Transaction struct {
	BatchNumber                 uint64 // 8 bytes
	EffectiveGasPricePercentage uint8  // 1 byte
	IsValid                     uint8  // 1 byte
	EncodedLength               uint32 // 4 bytes
	Encoded                     []byte
}

// Encode returns the encoded L2Transaction as a byte slice
func (l L2Transaction) Encode() []byte {
	var bigEndian = (*(*[2]uint8)(unsafe.Pointer(&[]uint16{1}[0])))[0] == 0
	bytes := make([]byte, 0)
	if bigEndian {
		bytes = binary.BigEndian.AppendUint64(bytes, l.BatchNumber)
	} else {
		bytes = binary.LittleEndian.AppendUint64(bytes, l.BatchNumber)
	}
	bytes = append(bytes, l.EffectiveGasPricePercentage)
	bytes = append(bytes, l.IsValid)
	if !bigEndian {
		bytes = binary.BigEndian.AppendUint32(bytes, l.EncodedLength)
	} else {
		bytes = binary.LittleEndian.AppendUint32(bytes, l.EncodedLength)
	}
	bytes = append(bytes, l.Encoded...)
	return bytes
}
