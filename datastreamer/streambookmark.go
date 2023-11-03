package datastreamer

import (
	"encoding/binary"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/syndtr/goleveldb/leveldb"
)

// StreamBookmark type to manage index of bookmarks
type StreamBookmark struct {
	dbName string
	db     *leveldb.DB
}

// NewBookmark creates bookmark struct and opens or creates the bookmark database
func NewBookmark(fn string) (*StreamBookmark, error) {
	b := StreamBookmark{
		dbName: fn,
		db:     nil,
	}

	// Open (or create) the bookmarks database
	log.Infof("Opening/creating bookmarks DB for datastream: %s", fn)
	db, err := leveldb.OpenFile(fn, nil)
	if err != nil {
		log.Errorf("Error opening or creating bookmarks DB %s: %v", fn, err)
		return nil, err
	}
	b.db = db

	return &b, nil
}

// AddBookmark inserts or updates a bookmark
func (b *StreamBookmark) AddBookmark(bookmark []byte, entryNum uint64) error {
	// Convert entry number to bytes slice
	var entry []byte
	entry = binary.BigEndian.AppendUint64(entry, entryNum)

	// Insert or update the bookmark into DB
	err := b.db.Put(bookmark, entry, nil)
	if err != nil {
		log.Errorf("Error inserting or updating bookmark [%v] value [%d]", bookmark, entryNum)
		return err
	}

	// Log
	log.Debugf("Bookmark added[%v] value[%d]", bookmark, entryNum)

	return nil
}

// GetBookmark gets a bookmark value
func (b *StreamBookmark) GetBookmark(bookmark []byte) (uint64, error) {
	// Get the bookmark from DB
	entry, err := b.db.Get(bookmark, nil)
	if err == leveldb.ErrNotFound {
		log.Infof("Bookmark not found [%v]: %v", bookmark, err)
		return 0, err
	} else if err != nil {
		log.Errorf("Error getting bookmark [%v]: %v", bookmark, err)
		return 0, err
	}

	// Convert bytes slice to entry number
	entryNum := binary.BigEndian.Uint64(entry)

	// Log
	log.Debugf("Bookmark got[%v] value[%d]", bookmark, entryNum)

	return entryNum, nil
}

// PrintDump prints all bookmarks stored in the database
func (b *StreamBookmark) PrintDump() error {
	// Counter
	var count uint64 = 0

	// Initialize iterator
	iter := b.db.NewIterator(nil, nil)

	// Iterator loop
	for iter.Next() {
		count++
		bookmark := iter.Key()
		entry := iter.Value()
		entryNum := binary.BigEndian.Uint64(entry)
		log.Debugf("Bookmark[%v] value[%d]", bookmark, entryNum)
	}

	// Check if error
	err := iter.Error()
	if err != nil {
		log.Errorf("Iterator error in PrintDump: %v", err)
	}

	// Finalize iterator
	iter.Release()

	// Log total
	log.Infof("Number of bookmarks: [%d]", count)

	return err
}
