package datastreamer_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/stretchr/testify/require"
)

func TestRelay(t *testing.T) {
	fileName1 := "/tmp/datastreamer_test_0.bin"
	fileName2 := "/tmp/datastreamer_test_1.bin"

	err := deleteFiles(fileName1)
	require.NoError(t, err)
	err = deleteFiles(fileName2)
	require.NoError(t, err)

	streamServer, err := datastreamer.NewServer(6901, 1, 137, streamType,
		fileName1, config.WriteTimeout, config.InactivityTimeout, 5*time.Second, &config.Log)
	require.NoError(t, err)

	err = streamServer.Start()
	require.NoError(t, err)
	err = streamServer.StartAtomicOp()
	require.NoError(t, err)

	entryNumber, err := streamServer.AddStreamBookmark(testBookmark.Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(0), entryNumber)

	entryNumber, err = streamServer.AddStreamEntry(entryType1, testEntries[1].Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(1), entryNumber)

	entryNumber, err = streamServer.AddStreamBookmark(testBookmark2.Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(2), entryNumber)

	entryNumber, err = streamServer.AddStreamEntry(entryType1, testEntries[2].Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(3), entryNumber)

	err = streamServer.CommitAtomicOp()
	require.NoError(t, err)

	var relayPort uint16 = 6902
	sr, err := datastreamer.NewRelay(fmt.Sprintf("localhost:%d", 6901), relayPort, 1, 137, datastreamer.StreamType(1),
		fileName2, config.WriteTimeout, config.InactivityTimeout, 5*time.Second, nil)
	require.NoError(t, err)
	err = sr.Start()
	require.NoError(t, err)

	client := datastreamer.NewClient(fmt.Sprintf("localhost:%d", relayPort), streamType)
	client.Start()
}
