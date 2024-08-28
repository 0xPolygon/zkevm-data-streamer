package datastreamer

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupTestFile(t *testing.T, filename string) *StreamFile {
	// Create a new stream file for testing
	sf, err := NewStreamFile(filename, 1, 12345, 1)
	assert.NoError(t, err)
	assert.NotNil(t, sf)

	return sf
}

func cleanupTestFile(filename string) {
	_ = os.Remove(filename)
}

func TestNewStreamFile(t *testing.T) {
	filename := "test_streamfile.dat"
	defer cleanupTestFile(filename)

	sf := setupTestFile(t, filename)

	// Check initial file settings
	assert.Equal(t, filename, sf.fileName)
	assert.Equal(t, uint32(PageDataSize), sf.pageSize)
	assert.Equal(t, uint64(4096), sf.header.TotalLength)
	assert.Equal(t, uint64(0), sf.header.TotalEntries)

	// Check if the file was created with the correct initial size
	info, err := os.Stat(filename)
	assert.NoError(t, err)
	assert.Equal(t, int64(PageHeaderSize+initPages*PageDataSize), info.Size())
}

func TestWriteAndReadHeader(t *testing.T) {
	filename := "test_streamfile_header.dat"
	defer cleanupTestFile(filename)

	sf := setupTestFile(t, filename)

	// Modify header and write it to the file
	sf.header.TotalEntries = 10
	sf.header.TotalLength = 4096
	err := sf.writeHeaderEntry()
	assert.NoError(t, err)

	// Read header back from the file
	err = sf.readHeaderEntry()
	assert.NoError(t, err)

	// Verify that the header data is correct
	assert.Equal(t, uint64(10), sf.header.TotalEntries)
	assert.Equal(t, uint64(4096), sf.header.TotalLength)
}
