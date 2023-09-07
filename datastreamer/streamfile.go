package datastreamer

import (
	"encoding/binary"
	"errors"
	"os"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
)

const (
	// File config
	headerSize     = 29          // Header data size
	pageHeaderSize = 4096        // 4K size header page
	pageSize       = 1024 * 1024 // 1 MB size data page
	initPages      = 80          // Initial number of data pages
	nextPages      = 8           // Number of data pages to add when file is full

	// Packet types
	PtPadding = 0
	PtHeader  = 1    // Just for the header page
	PtEntry   = 2    // Data entry
	PtResult  = 0xff // Not stored/present in file (for client command result)

	// Sizes
	FixedSizeFileEntry   = 17 // 1+4+4+8
	FixedSizeResultEntry = 9  // 1+4+4
)

type HeaderEntry struct {
	packetType   uint8  // 1:Header
	headLength   uint32 // 29
	streamType   uint64 // 1:Sequencer
	totalLength  uint64 // Total bytes used in the file
	totalEntries uint64 // Total number of data entries (entry type 2)
}

type FileEntry struct {
	packetType uint8     // 2:Data entry, 0:Padding, (1:Header)
	length     uint32    // Length of the entry
	entryType  EntryType // 1:L2 block, 2:L2 tx
	entryNum   uint64    // Entry number (sequential starting with 0)
	data       []byte
}

type StreamFile struct {
	fileName   string
	pageSize   uint32 // in bytes
	file       *os.File
	streamType uint64
	maxLength  uint64 // File size

	header HeaderEntry
}

func PrepareStreamFile(fn string, st uint64) (StreamFile, error) {
	sf := StreamFile{
		fileName:   fn,
		pageSize:   pageSize,
		file:       nil,
		streamType: st,
		maxLength:  0,

		header: HeaderEntry{
			packetType:   PtHeader,
			headLength:   headerSize,
			streamType:   st,
			totalLength:  0,
			totalEntries: 0,
		},
	}

	// Open (or create) the data stream file
	err := sf.openCreateFile()

	printStreamFile(sf)

	return sf, err
}

func (f *StreamFile) openCreateFile() error {
	// Check if file exists (otherwise create it)
	_, err := os.Stat(f.fileName)

	if os.IsNotExist(err) {
		// File does not exists so create it
		log.Info("Creating file for datastream: ", f.fileName)
		f.file, err = os.Create(f.fileName)

		if err != nil {
			log.Error("Error creating datastream file: ", f.fileName, err)
		} else {
			err = f.initializeFile()
		}

	} else if err == nil {
		// File already exists
		log.Info("File for datastream already exists: ", f.fileName)
		f.file, err = os.OpenFile(f.fileName, os.O_RDWR, 0666)
		if err != nil {
			log.Error("Error opening datastream file: ", f.fileName, err)
		}
	} else {
		log.Error("Unable to check datastream file status: ", f.fileName, err)
	}

	if err != nil {
		return err
	}

	// Max length of the file
	info, err := f.file.Stat()
	if err != nil {
		return err
	}
	f.maxLength = uint64(info.Size())

	// Check file consistency
	err = f.checkFileConsistency()
	if err != nil {
		return err
	}

	// Restore header from the file and check it
	err = f.readHeaderEntry()
	if err != nil {
		return err
	}
	err = f.checkHeaderConsistency()
	if err != nil {
		return err
	}

	return nil
}

func (f *StreamFile) initializeFile() error {
	// Create the header page
	err := f.createHeaderPage()
	if err != nil {
		return err
	}

	// Create initial data pages
	for i := 1; i <= initPages; i++ {
		err = f.createPage(f.pageSize)
		if err != nil {
			log.Error("Eror creating page")
			return err
		}
	}

	return err
}

func (f *StreamFile) createHeaderPage() error {
	// Create the header page (first page) of the file
	err := f.createPage(pageHeaderSize)
	if err != nil {
		log.Error("Error creating the header page: ", err)
		return err
	}

	// Update total data length and max file length
	f.maxLength = f.maxLength + pageHeaderSize
	f.header.totalLength = pageHeaderSize

	// Write header entry
	err = f.writeHeaderEntry()
	return err
}

// Create/add a new page on the stream file
func (f *StreamFile) createPage(size uint32) error {
	page := make([]byte, size)

	// Position at the end of the file
	_, err := f.file.Seek(0, 2)
	if err != nil {
		log.Error("Error seeking the end of the file: ", err)
		return err
	}

	// Write the page
	_, err = f.file.Write(page)
	if err != nil {
		log.Error("Error writing a new page: ", err)
		return err
	}

	// Flush
	err = f.file.Sync()
	if err != nil {
		log.Error("Error flushing new page to disk: ", err)
		return err
	}

	// Update max file length
	f.maxLength = f.maxLength + uint64(size)

	return nil
}

func (f *StreamFile) extendFile() error {
	// Add data pages
	var err error = nil
	for i := 1; i <= nextPages; i++ {
		err = f.createPage(f.pageSize)
		if err != nil {
			log.Error("Error adding page")
			return err
		}
	}
	return err
}

func (f *StreamFile) readHeaderEntry() error {
	_, err := f.file.Seek(0, 0)
	if err != nil {
		log.Error("Error seeking the start of the file: ", err)
		return err
	}

	binaryHeader := make([]byte, headerSize)
	n, err := f.file.Read(binaryHeader)
	if err != nil {
		log.Error("Error reading the header: ", err)
		return err
	}
	if n != headerSize {
		log.Error("Error getting header info")
		return errors.New("error getting header info")
	}

	f.header, err = decodeBinaryToHeaderEntry(binaryHeader)
	if err != nil {
		log.Error("Error decoding binary header")
		return err
	}
	return nil
}

func printHeaderEntry(e HeaderEntry) {
	log.Debug("--- HEADER ENTRY -------------------------")
	log.Debugf("packetType: [%d]", e.packetType)
	log.Debugf("headerLength: [%d]", e.headLength)
	log.Debugf("streamType: [%d]", e.streamType)
	log.Debugf("totalLength: [%d]", e.totalLength)
	log.Debugf("totalEntries: [%d]", e.totalEntries)
}

func (f *StreamFile) writeHeaderEntry() error {
	_, err := f.file.Seek(0, 0)
	if err != nil {
		log.Error("Error seeking the start of the file: ", err)
		return err
	}

	binaryHeader := encodeHeaderEntryToBinary(f.header)
	log.Debug("writing header entry: ", binaryHeader)
	_, err = f.file.Write(binaryHeader)
	if err != nil {
		log.Error("Error writing the header: ", err)
	}
	err = f.file.Sync()
	if err != nil {
		log.Error("Error flushing header data to disk: ", err)
	}

	return err
}

// Encode/convert from a header entry type to binary bytes slice
func encodeHeaderEntryToBinary(e HeaderEntry) []byte {
	be := make([]byte, 1)
	be[0] = e.packetType
	be = binary.BigEndian.AppendUint32(be, e.headLength)
	be = binary.BigEndian.AppendUint64(be, e.streamType)
	be = binary.BigEndian.AppendUint64(be, e.totalLength)
	be = binary.BigEndian.AppendUint64(be, e.totalEntries)
	return be
}

// Decode/convert from binary bytes slice to a header entry type
func decodeBinaryToHeaderEntry(b []byte) (HeaderEntry, error) {
	e := HeaderEntry{}

	if len(b) != headerSize {
		log.Error("Invalid binary header entry")
		return e, errors.New("invalid binary header entry")
	}

	e.packetType = b[0]
	e.headLength = binary.BigEndian.Uint32(b[1:5])
	e.streamType = binary.BigEndian.Uint64(b[5:13])
	e.totalLength = binary.BigEndian.Uint64(b[13:21])
	e.totalEntries = binary.BigEndian.Uint64(b[21:29])

	return e, nil
}

func encodeFileEntryToBinary(e FileEntry) []byte {
	be := make([]byte, 1)
	be[0] = e.packetType
	be = binary.BigEndian.AppendUint32(be, e.length)
	be = binary.BigEndian.AppendUint32(be, uint32(e.entryType))
	be = binary.BigEndian.AppendUint64(be, e.entryNum)
	be = append(be, e.data...)
	return be
}

func (f *StreamFile) checkFileConsistency() error {
	info, err := os.Stat(f.fileName)
	if err != nil {
		log.Error("Error checking file consistency")
		return err
	}

	if info.Size() < pageHeaderSize {
		log.Error("Invalid file: missing header page")
		return errors.New("invalid file missing header page")
	}

	dataSize := info.Size() - pageHeaderSize
	uncut := dataSize % int64(f.pageSize)
	if uncut != 0 {
		log.Error("Inconsistent file size there is a cut data page")
		return errors.New("bad file size cut data page")
	}

	return nil
}

func (f *StreamFile) checkHeaderConsistency() error {
	var err error = nil

	if f.header.packetType != PtHeader {
		log.Error("Invalid header: bad packet type")
		err = errors.New("invalid header bad packet type")
	} else if f.header.headLength != headerSize {
		log.Error("Invalid header: bad header length")
		err = errors.New("invalid header bad header length")
	} else if f.header.streamType != StSequencer {
		log.Error("Invalid header: bad stream type")
		err = errors.New("invalid header bad stream type")
	}

	return err
}

func (f *StreamFile) AddFileEntry(e FileEntry) error {
	// Set the file position to write
	_, err := f.file.Seek(int64(f.header.totalLength), 0)
	if err != nil {
		log.Error("Error seeking position to write:", err)
		return err
	}

	// Binary entry
	be := encodeFileEntryToBinary(e)

	// Check if the entry fits on current page
	entryLength := uint64(len(be))
	pageRemaining := pageSize - (f.header.totalLength-pageHeaderSize)%pageSize
	if entryLength > pageRemaining {
		log.Debug("== Fill with padd entries")
		err = f.fillPagePaddEntries()
		if err != nil {
			return err
		}

		// Add new data pages to the file
		if f.header.totalLength == f.maxLength {
			log.Info("== FULL FILE -> extending!")
			err = f.extendFile()
			if err != nil {
				return err
			}

			log.Info("New file max length: ", f.maxLength)

			// Re-set the file position to write
			_, err = f.file.Seek(int64(f.header.totalLength), 0)
			if err != nil {
				log.Error("Error seeking position to write after file extend: ", err)
				return err
			}
		}
	}

	// Write the entry
	_, err = f.file.Write(be)
	if err != nil {
		log.Error("Error writing the entry: ", err)
		return err
	}

	// Flush entry
	err = f.file.Sync()
	if err != nil {
		log.Error("Error flushing new entry to disk: ", err)
		return err
	}

	// Update the header just in memory (on disk when the commit arrives)
	f.header.totalLength = f.header.totalLength + entryLength
	f.header.totalEntries = f.header.totalEntries + 1

	// printHeaderEntry(f.header)
	return nil
}

func (f *StreamFile) fillPagePaddEntries() error {
	// Set the file position to write
	_, err := f.file.Seek(int64(f.header.totalLength), 0)
	if err != nil {
		log.Error("Error seeking fill padds position to write: ", err)
		return err
	}

	// Page remaining to fill with padd entries
	pageRemaining := pageSize - (f.header.totalLength-pageHeaderSize)%pageSize

	if pageRemaining > 0 {
		// Padd entries
		entries := make([]byte, pageRemaining)
		for i := 0; i < int(pageRemaining); i++ {
			entries[i] = 0
		}

		// Write padd entries
		_, err = f.file.Write(entries)
		if err != nil {
			log.Error("Error writing padd entries: ", err)
			return err
		}

		// Sync/flush to disk will be done outside this function

		// Update the header just in memory (on disk when the commit arrives)
		f.header.totalLength = f.header.totalLength + pageRemaining
		// f.header.totalEntries = f.header.totalEntries + pageRemaining
	}

	return nil
}

func printStreamFile(f StreamFile) {
	log.Debug("--- STREAM FILE --------------------------")
	log.Debugf("fileName: [%s]", f.fileName)
	log.Debugf("pageSize: [%d]", f.pageSize)
	log.Debugf("streamType: [%d]", f.streamType)
	log.Debugf("maxLength: [%d]", f.maxLength)
	printHeaderEntry(f.header)
}

// Decode/convert from binary bytes slice to file entry type
func DecodeBinaryToFileEntry(b []byte) (FileEntry, error) {
	d := FileEntry{}

	if len(b) < FixedSizeFileEntry {
		log.Error("Invalid binary data entry")
		return d, errors.New("invalid binary data entry")
	}

	d.packetType = b[0]
	d.length = binary.BigEndian.Uint32(b[1:5])
	d.entryType = EntryType(binary.BigEndian.Uint32(b[5:9]))
	d.entryNum = binary.BigEndian.Uint64(b[9:17])
	d.data = b[17:]

	if uint32(len(d.data)) != d.length-FixedSizeFileEntry {
		log.Error("Error decoding binary data entry")
		return d, errors.New("error decoding binary data entry")
	}

	return d, nil
}
