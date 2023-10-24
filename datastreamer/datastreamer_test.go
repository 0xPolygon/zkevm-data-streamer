package datastreamer_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

type TestEntry struct {
	FieldA uint64      // 8 bytes
	FieldB common.Hash // 32 bytes
	FieldC []byte      // n bytes
}

type TestBookmark struct {
	FieldA []byte
}

func (t TestEntry) Encode() []byte {
	bytes := make([]byte, 0)
	bytes = binary.LittleEndian.AppendUint64(bytes, t.FieldA)
	bytes = append(bytes, t.FieldB[:]...)
	bytes = append(bytes, t.FieldC[:]...)
	return bytes
}

func (t TestEntry) Decode(bytes []byte) TestEntry {
	t.FieldA = binary.LittleEndian.Uint64(bytes[:8])
	t.FieldB = common.BytesToHash(bytes[8:40])
	t.FieldC = bytes[40:]
	return t
}

func (t TestBookmark) Encode() []byte {
	return t.FieldA
}

var (
	config = datastreamer.Config{
		Port:     6900,
		Filename: "/tmp/datastreamer_test.bin",
		Log: log.Config{
			Environment: "development",
			Level:       "debug",
			Outputs:     []string{"stdout"},
		},
	}
	leveldb       = config.Filename[0:strings.IndexRune(config.Filename, '.')] + ".db"
	streamServer  datastreamer.StreamServer
	streamType    = datastreamer.StreamType(1)
	testEntryType = datastreamer.EntryType(1)

	testEntry = TestEntry{
		FieldA: 123,
		FieldB: common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
		FieldC: []byte("test entry 1"),
	}

	testEntry2 = TestEntry{
		FieldA: 456,
		FieldB: common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
		FieldC: []byte("test entry 2"),
	}

	testBookmark = TestBookmark{
		FieldA: []byte{0, 1, 0, 0, 0, 0, 0, 0, 0},
	}
)

func deleteFiles() error {
	// Delete test file from filesystem
	err := os.Remove(config.Filename)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// Delete leveldb folder from filesystem
	err = os.RemoveAll(leveldb)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func TestServer(t *testing.T) {
	err := deleteFiles()
	if err != nil {
		panic(err)
	}
	streamServer, err = datastreamer.NewServer(config.Port, streamType, config.Filename, &config.Log)
	if err != nil {
		panic(err)
	}
	// Should fail because the start atomic operation has not been called
	entryNumber, err := streamServer.AddStreamEntry(testEntryType, testEntry.Encode())
	require.Equal(t, datastreamer.ErrAddEntryNotAllowed, err)
	require.Equal(t, uint64(0), entryNumber)

	// Should fail because server is not started
	err = streamServer.StartAtomicOp()
	require.Equal(t, datastreamer.ErrAtomicOpNotAllowed, err)
	require.Equal(t, uint64(0), entryNumber)

	// Should succeed
	err = streamServer.Start()
	require.NoError(t, err)

	// Should succeed
	err = streamServer.StartAtomicOp()
	require.NoError(t, err)

	// Should succeed
	entryNumber, err = streamServer.AddStreamBookmark(testBookmark.Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(0), entryNumber)

	entryNumber, err = streamServer.AddStreamEntry(testEntryType, testEntry.Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(1), entryNumber)

	entryNumber, err = streamServer.AddStreamEntry(testEntryType, testEntry2.Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(2), entryNumber)

	err = streamServer.CommitAtomicOp()
	require.NoError(t, err)

	// Get the second entry
	entry, err := streamServer.GetEntry(2)
	require.NoError(t, err)
	require.Equal(t, testEntry2, TestEntry{}.Decode(entry.Data))
}

func TestClient(t *testing.T) {
	client, err := datastreamer.NewClient(fmt.Sprintf("localhost:%d", config.Port), streamType)
	require.NoError(t, err)

	err = client.Start()
	require.NoError(t, err)

	client.FromBookmark = testBookmark.FieldA
	err = client.ExecCommand(datastreamer.CmdBookmark)
	require.NoError(t, err)

	client.FromEntry = 2
	err = client.ExecCommand(datastreamer.CmdEntry)
	require.NoError(t, err)

	require.Equal(t, testEntry2, TestEntry{}.Decode(client.Entry.Data))

	client.FromEntry = 1
	err = client.ExecCommand(datastreamer.CmdEntry)
	require.NoError(t, err)
	require.Equal(t, testEntry, TestEntry{}.Decode(client.Entry.Data))
}
