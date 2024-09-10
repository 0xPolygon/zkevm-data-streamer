package datastreamer_test

import (
	"testing"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/stretchr/testify/require"
)

func TestOpenFile(t *testing.T) {
	fileName := "/tmp/datastreamer_test_3.bin"
	err := deleteFiles(fileName)
	require.NoError(t, err)
	_, err = datastreamer.NewStreamFile(fileName, 1, 137, streamType)
	require.NoError(t, err)

	_, err = datastreamer.NewStreamFile(fileName, 1, 137, streamType)
	require.NoError(t, err)

	err = deleteFiles(fileName)
	require.NoError(t, err)
}
