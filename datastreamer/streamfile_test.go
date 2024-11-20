package datastreamer

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupTestFile(t *testing.T, filename string) *StreamFile {
	t.Helper()

	sf, err := NewStreamFile(filename, 1, 12345, 1)
	assert.NoError(t, err)
	assert.NotNil(t, sf)

	return sf
}

func cleanupTestFile(filename string) {
	_ = os.Remove(filename)
}

func TestNewStreamFile(t *testing.T) {
	filename := "test_streamfile.bin"
	defer cleanupTestFile(filename)

	sf := setupTestFile(t, filename)

	assert.Equal(t, filename, sf.fileName)
	assert.Equal(t, uint32(PageDataSize), sf.pageSize)
	assert.Equal(t, uint64(4096), sf.header.TotalLength)
	assert.Equal(t, uint64(0), sf.header.TotalEntries)

	info, err := os.Stat(filename)
	assert.NoError(t, err)
	assert.Equal(t, int64(PageHeaderSize+initPages*PageDataSize), info.Size())
}

func TestWriteAndReadHeader(t *testing.T) {
	filename := "test_streamfile_header.bin"
	defer cleanupTestFile(filename)

	sf := setupTestFile(t, filename)

	sf.header.TotalEntries = 10
	sf.header.TotalLength = 4096
	err := sf.writeHeaderEntry()
	assert.NoError(t, err)

	err = sf.readHeaderEntry()
	assert.NoError(t, err)

	assert.Equal(t, uint64(10), sf.header.TotalEntries)
	assert.Equal(t, uint64(4096), sf.header.TotalLength)
}
