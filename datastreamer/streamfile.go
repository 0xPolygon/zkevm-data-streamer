package datastreamer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

const (
	// Packet type
	ptSequencer = 1 // Sequencer

	// File config
	headerSize     = 29          // Header data size
	pageHeaderSize = 4096        // 4K size header page
	pageSize       = 1024 * 1024 // 1 MB size data page
	initPages      = 8           // Initial number of data pages
	nextPages      = 8           // Number of data pages to add when run out

	// Is Entry values
	IEPadding = 0
	IEHeader  = 1
	IEEntry   = 2
)

type HeaderEntry struct {
	packetType   uint8
	headLength   uint32
	streamType   uint64
	totalLength  uint64
	totalEntries uint64
}

type FileEntry struct {
	isEntry   uint8  // 0:Padding, 1:Header, 2:Entry
	length    uint32 // Length of the entry
	entryType uint32 // 1:Tx, 2:Batch-start
	entryNum  uint64 // Entry sequential number (starts with 0)
	data      []byte
}

type StreamFile struct {
	fileName   string
	pageSize   uint32 // in bytes
	file       *os.File
	streamType uint64

	header HeaderEntry

	totalPages        uint64 // Total number of data pages created in the file
	currentPage       uint64 // Current number of data page to write next entry
	currentOffset     uint64 // Offset of current data page to write next entry
	uncommittedPage   uint64 // Initial uncommitted data page
	uncommittedOffset uint64 // Offset of initial uncommitted data page
}

func PrepareStreamFile(fn string, st uint64) (StreamFile, error) {
	sf := StreamFile{
		fileName:   fn,
		pageSize:   pageSize,
		file:       nil,
		streamType: st,

		header: HeaderEntry{
			packetType:   ptSequencer,
			headLength:   headerSize,
			streamType:   st,
			totalLength:  0,
			totalEntries: 0,
		},

		totalPages:        0,
		currentPage:       1,
		currentOffset:     0,
		uncommittedPage:   1,
		uncommittedOffset: 0,
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
		fmt.Println("Creating file for datastream:", f.fileName)
		f.file, err = os.Create(f.fileName)

		if err != nil {
			fmt.Println("Error creating datastream file:", f.fileName, err)
		} else {
			err = f.initializeFile()
		}

	} else if err == nil {
		// File already exists
		fmt.Println("File for datastream already exists:", f.fileName)
		f.file, err = os.OpenFile(f.fileName, os.O_APPEND, 0666)
		if err != nil {
			fmt.Println("Error opening datastream file:", f.fileName, err)
		}
	} else {
		fmt.Println("Unable to check datastream file status:", f.fileName, err)
	}

	if err != nil {
		return err
	}

	// Check file consistency
	err = f.checkFileConsistency()
	if err != nil {
		return err
	}
	fmt.Println("Number of pages:", f.totalPages)

	// Restore header from the file and check it
	err = f.readHeaderEntry()
	if err != nil {
		return err
	}

	err = f.checkHeaderConsistency()
	if err != nil {
		return err
	}

	// Calculate current page and offset
	err = f.calculateCurrentPageOffset()
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
		err = f.createPage(f.pageSize, true)
		if err != nil {
			fmt.Println("Eror creating page:", f.totalPages+1)
			return err
		}
	}

	return err
}

func (f *StreamFile) createHeaderPage() error {
	// Create the header page (first page) of the file
	err := f.createPage(pageHeaderSize, false)
	if err != nil {
		fmt.Println("Error creating the header page:", err)
		return err
	}

	f.header.totalLength = pageHeaderSize

	// Update the header entry
	err = f.writeHeaderEntry()
	return err
}

// Create/add a new page on the stream file
func (f *StreamFile) createPage(size uint32, isDataPage bool) error {
	page := make([]byte, size)

	// Position at the end of the file
	_, err := f.file.Seek(0, 2)
	if err != nil {
		fmt.Println("Error seeking the end of the file:", err)
		return err
	}

	// Write the page
	_, err = f.file.Write(page)
	if err != nil {
		fmt.Println("Error writing a new page:", err)
		return err
	}

	// Flush
	err = f.file.Sync()
	if err != nil {
		fmt.Println("Error flushing new page to disk:", err)
		return err
	}

	if isDataPage {
		f.totalPages++
	}
	return nil
}

func (f *StreamFile) readHeaderEntry() error {
	_, err := f.file.Seek(0, 0)
	if err != nil {
		fmt.Println("Error seeking the start of the file:", err)
		return err
	}

	binaryHeader := make([]byte, headerSize)
	n, err := f.file.Read(binaryHeader)
	if err != nil {
		fmt.Println("Error reading the header:", err)
		return err
	}
	if n != headerSize {
		fmt.Println("Error getting header info")
		return errors.New("error getting header info")
	}

	f.header, err = decodeBinaryToHeaderEntry(binaryHeader)
	if err != nil {
		fmt.Println("Error decoding binary header")
		return err
	}
	return nil
}

func printHeaderEntry(e HeaderEntry) {
	fmt.Println("  --- HEADER ENTRY -------------------------")
	fmt.Printf("  packetType: [%d]\n", e.packetType)
	fmt.Printf("  headerLength: [%d]\n", e.headLength)
	fmt.Printf("  streamType: [%d]\n", e.streamType)
	fmt.Printf("  totalLength: [%d]\n", e.totalLength)
	fmt.Printf("  totalEntries: [%d]\n", e.totalEntries)
}

func (f *StreamFile) writeHeaderEntry() error {
	_, err := f.file.Seek(0, 0)
	if err != nil {
		fmt.Println("Error seeking the start of the file:", err)
		return err
	}

	binaryHeader := encodeHeaderEntryToBinary(f.header)
	fmt.Println("writing header entry:", binaryHeader)
	_, err = f.file.Write(binaryHeader)
	if err != nil {
		fmt.Println("Error writing the header:", err)
	}
	err = f.file.Sync()
	if err != nil {
		fmt.Println("Error flushing header data to disk:", err)
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
		fmt.Println("Invalid binary header entryy")
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
	be[0] = e.isEntry
	be = binary.BigEndian.AppendUint32(be, e.length)
	be = binary.BigEndian.AppendUint32(be, e.entryType)
	be = binary.BigEndian.AppendUint64(be, e.entryNum)
	be = append(be, e.data...)
	return be
}

func (f *StreamFile) checkFileConsistency() error {
	info, err := os.Stat(f.fileName)
	if err != nil {
		fmt.Println("Error checking file consistency")
		return err
	}

	if info.Size() < pageHeaderSize {
		fmt.Println("Invalid file: missing header page")
		return errors.New("invalid file missing header page")
	}

	dataSize := info.Size() - pageHeaderSize
	uncut := dataSize % int64(f.pageSize)
	if uncut != 0 {
		fmt.Println("Inconsistent file size there is a cut data page")
		return errors.New("bad file size cut data page")
	}

	f.totalPages = uint64(dataSize) / uint64(f.pageSize)

	return nil
}

func (f *StreamFile) checkHeaderConsistency() error {
	var err error = nil

	if f.header.packetType != ptSequencer {
		fmt.Println("Invalid header: bad packet type")
		err = errors.New("invalid header bad packet type")
	} else if f.header.headLength != headerSize {
		fmt.Println("Invalid header: bad header length")
		err = errors.New("invalid header bad header length")
	} else if f.header.streamType != f.streamType {
		fmt.Println("Invalid header: bad stream type")
		err = errors.New("invalid header bad stream type")
	} else if f.header.totalLength > f.totalPages*uint64(f.pageSize) {
		fmt.Println("Invalid header: bad total length")
		err = errors.New("invalid header bad total length")
	}

	return err
}

func (f *StreamFile) calculateCurrentPageOffset() error {
	// Calculate/restore current page and offset from the header total length
	dataLength := f.header.totalLength + 1 - pageHeaderSize
	f.currentPage = dataLength / uint64(f.pageSize)
	f.currentOffset = dataLength % uint64(f.pageSize)

	if f.currentPage > f.totalPages {
		fmt.Println("Bad total length in header")
		return errors.New("bad total length header")
	}
	return nil
}

func (f *StreamFile) AddFileEntry(e FileEntry) error {
	// Set the file position to write
	pos := pageHeaderSize + f.currentPage*uint64(f.pageSize) + f.currentOffset
	_, err := f.file.Seek(int64(pos), 0)
	if err != nil {
		fmt.Println("Error seeking position to write:", err)
		return err
	}

	// TODO: check if the entry fits on current page

	// Write the entry
	be := encodeFileEntryToBinary(e)
	_, err = f.file.Write(be)
	if err != nil {
		fmt.Println("Error writing the entry:", err)
		return err
	}

	// Flush entry
	err = f.file.Sync()
	if err != nil {
		fmt.Println("Error flushing new entry to disk:", err)
		return err
	}

	// Update current offset
	f.currentOffset = f.currentOffset + uint64(len(be))

	return nil
}

func (f *StreamFile) StartFileTx() {
	f.uncommittedPage = f.currentPage
	f.uncommittedOffset = f.currentOffset
}

func printStreamFile(f StreamFile) {
	fmt.Println("  --- STREAM FILE- -------------------------")
	fmt.Printf("  fileName: [%s]\n", f.fileName)
	fmt.Printf("  pageSize: [%d]\n", f.pageSize)
	fmt.Printf("  streamType: [%d]\n", f.streamType)
	fmt.Printf("  totalPages: [%d]\n", f.totalPages)
	fmt.Printf("  currentPage: [%d]\n", f.currentPage)
	fmt.Printf("  currentOffset: [%d]\n", f.currentOffset)
	fmt.Printf("  uncommittedPage: [%d]\n", f.uncommittedPage)
	fmt.Printf("  uncommittedOffset: [%d]\n", f.uncommittedOffset)
	printHeaderEntry(f.header)
}
