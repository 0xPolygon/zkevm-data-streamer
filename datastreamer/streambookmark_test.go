package datastreamer

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func createTempDB(t *testing.T) *StreamBookmark {
	t.Helper()

	tempFile := "test_bookmarks_db"
	b, err := NewBookmark(tempFile)
	if err != nil {
		t.Fatalf("Failed to create bookmark: %v", err)
	}

	return b
}

func cleanUpDB(t *testing.T, b *StreamBookmark) {
	t.Helper()

	err := b.db.Close()
	if err != nil {
		t.Fatalf("Failed to close bookmark database: %v", err)
	}

	err = os.RemoveAll(b.dbName)
	if err != nil {
		t.Fatalf("Failed to remove test database: %v", err)
	}
}

func TestNewBookmark(t *testing.T) {
	b := createTempDB(t)
	defer cleanUpDB(t, b)

	if b == nil || b.db == nil {
		t.Fatalf("Expected non-nil bookmark and database")
	}
}

func TestAddBookmark(t *testing.T) {
	b := createTempDB(t)
	defer cleanUpDB(t, b)

	bookmark := []byte("testBookmark")
	entryNum := uint64(12345)

	err := b.AddBookmark(bookmark, entryNum)
	if err != nil {
		t.Fatalf("Failed to add bookmark: %v", err)
	}

	value, err := b.GetBookmark(bookmark)
	if err != nil {
		t.Fatalf("Failed to get bookmark after adding: %v", err)
	}

	assert.Equal(t, entryNum, value, "Expected bookmark value %d, got %d", entryNum, value)
}

func TestGetBookmarkNotFound(t *testing.T) {
	b := createTempDB(t)
	defer cleanUpDB(t, b)

	nonExistentBookmark := []byte("nonExistentBookmark")
	_, err := b.GetBookmark(nonExistentBookmark)
	assert.Error(t, err, "Expected error when getting a non-existent bookmark")
}
