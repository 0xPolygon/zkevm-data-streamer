package datastreamer

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"os"
	"sync"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
)

var (
	magicNumbers = []byte("polygonDATSTREAM")
)

const (
	fileMode       = 0666        // Open file mode
	magicNumSize   = 16          // Magic numbers size
	headerSize     = 38          // Header data size
	PageHeaderSize = 4096        // PageHeaderSize is the size of header page (4 KB)
	PageDataSize   = 1024 * 1024 // PageDataSize is the size of one data page (1 MB)
	initPages      = 100         // Initial number of data pages
	nextPages      = 10          // Number of data pages to add when file is full

	PtPadding = 0    // PtPadding is packet type for pad
	PtHeader  = 1    // PtHeader is packet type just for the header page
	PtData    = 2    // PtData is packet type for data entry
	PtDataRsp = 0xfe // PtDataRsp is packet type for command response with data
	PtResult  = 0xff // PtResult is packet type not stored/present in file (just for client command result)

	EtBookmark = 0xb0 // EtBookmark is entry type for bookmarks

	FixedSizeFileEntry   = 17 // FixedSizeFileEntry is the fixed size in bytes for a data file entry (1+4+4+8)
	FixedSizeResultEntry = 9  // FixedSizeResultEntry is the fixed size in bytes for a result entry (1+4+4)
)

// HeaderEntry type for a header entry
type HeaderEntry struct {
	packetType   uint8      // 1:Header
	headLength   uint32     // Total length of header entry (38)
	Version      uint8      // Stream file version
	SystemID     uint64     // System identifier (e.g. ChainID)
	streamType   StreamType // 1:Sequencer
	TotalLength  uint64     // Total bytes used in the file
	TotalEntries uint64     // Total number of data entries (packet type PtData)
}

// FileEntry type for a data file entry
type FileEntry struct {
	packetType uint8     // 2:Data entry, 0:Padding
	Length     uint32    // Total length of the entry (17 bytes + length(data))
	Type       EntryType // 0xb0:Bookmark, 1:Event1, 2:Event2,...
	Number     uint64    // Entry number (sequential starting with 0)
	Data       []byte
}

// StreamFile type to manage a binary stream file
type StreamFile struct {
	fileName   string
	pageSize   uint32 // Data page size in bytes
	file       *os.File
	streamType StreamType
	maxLength  uint64 // File size in bytes

	fileHeader  *os.File    // File descriptor just for read/write the header
	header      HeaderEntry // Current header in memory (atomic operation in progress)
	writtenHead HeaderEntry // Current header written in the file
	mutexHeader sync.Mutex  // Mutex for update header data
}

type iteratorFile struct {
	fromEntry uint64
	file      *os.File
	Entry     FileEntry
}

// NewStreamFile creates stream file struct and opens or creates the stream binary data file
func NewStreamFile(fn string, version uint8, systemID uint64, st StreamType) (*StreamFile, error) {
	sf := StreamFile{
		fileName:   fn,
		pageSize:   PageDataSize,
		file:       nil,
		streamType: st,
		maxLength:  0,

		fileHeader: nil,
		header: HeaderEntry{
			packetType:   PtHeader,
			headLength:   headerSize,
			Version:      version,
			SystemID:     systemID,
			streamType:   st,
			TotalLength:  0,
			TotalEntries: 0,
		},
	}

	// Open (or create) the data stream file
	err := sf.openCreateFile()
	if err != nil {
		return nil, err
	}

	// Print file info
	printStreamFile(&sf)

	return &sf, err
}

// openCreateFile opens or creates the stream file and performs multiple checks
func (f *StreamFile) openCreateFile() error {
	// Check if file exists (otherwise create it)
	_, err := os.Stat(f.fileName)

	if os.IsNotExist(err) {
		// File does not exists so create it
		log.Infof("Creating new file for datastream: %s", f.fileName)
		f.file, err = os.Create(f.fileName)

		if err != nil {
			log.Errorf("Error creating datastream file %s: %v", f.fileName, err)
		} else {
			err = f.openFileForHeader()
			if err != nil {
				return err
			}
			err = f.initializeFile()
		}
	} else if err == nil {
		// File already exists
		log.Infof("Using existing file for datastream: %s", f.fileName)
		f.file, err = os.OpenFile(f.fileName, os.O_RDWR, fileMode)
		if err != nil {
			log.Errorf("Error opening datastream file %s: %v", f.fileName, err)
			return err
		}

		err = f.openFileForHeader()
	} else {
		log.Errorf("Unable to check datastream file status %s: %v", f.fileName, err)
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

	// Check magic numbers
	err = f.checkMagicNumbers()
	if err != nil {
		return err
	}

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

	// Set initial file position to write
	_, err = f.file.Seek(int64(f.header.TotalLength), io.SeekStart)
	if err != nil {
		log.Errorf("Error seeking starting position to write: %v", err)
		return err
	}

	return nil
}

// openFileForHeader opens stream file to perform header operations
func (f *StreamFile) openFileForHeader() error {
	// Get another file descriptor to use just for read/write the header
	var err error
	f.fileHeader, err = os.OpenFile(f.fileName, os.O_RDWR, fileMode)
	if err != nil {
		log.Errorf("Error opening file for read/write header: %v", err)
		return err
	}
	return nil
}

// initializeFile creates and initializes the stream file structure
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

// createHeaderPage creates and initilize the header page of the stream file
func (f *StreamFile) createHeaderPage() error {
	// Create the header page (first page) of the file
	err := f.createPage(PageHeaderSize)
	if err != nil {
		log.Errorf("Error creating the header page: %v", err)
		return err
	}

	// Update total data length and max file length
	f.mutexHeader.Lock()
	f.maxLength = f.maxLength + PageHeaderSize
	f.header.TotalLength = PageHeaderSize
	f.mutexHeader.Unlock()

	// Write magic numbers
	err = f.writeMagicNumbers()
	if err != nil {
		return err
	}

	// Write header entry
	err = f.writeHeaderEntry()
	return err
}

// writeMagicNumbers writes the magic bytes at the beginning of the header page
func (f *StreamFile) writeMagicNumbers() error {
	// Position at the start of the file
	_, err := f.fileHeader.Seek(0, io.SeekStart)
	if err != nil {
		log.Errorf("Error seeking the end of the file: %v", err)
		return err
	}

	// Write the magic numbers
	_, err = f.fileHeader.Write(magicNumbers)
	if err != nil {
		log.Errorf("Error writing magic numbers: %v", err)
		return err
	}

	return nil
}

// createPage creates (adds) a new page on the stream file
func (f *StreamFile) createPage(size uint32) error {
	page := make([]byte, size)

	// Position at the end of the file
	_, err := f.file.Seek(0, io.SeekEnd)
	if err != nil {
		log.Errorf("Error seeking the end of the file: %v", err)
		return err
	}

	// Write the page
	_, err = f.file.Write(page)
	if err != nil {
		log.Errorf("Error writing a new page: %v", err)
		return err
	}

	// Flush
	err = f.file.Sync()
	if err != nil {
		log.Errorf("Error flushing new page to disk: %v", err)
		return err
	}

	// Update max file length
	f.maxLength = f.maxLength + uint64(size)

	return nil
}

// extendFile extends the stream file by adding new data pages
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

// readHeaderEntry reads header from file to restore the header struct
func (f *StreamFile) readHeaderEntry() error {
	// Position at the beginning of the file
	_, err := f.fileHeader.Seek(magicNumSize, io.SeekStart)
	if err != nil {
		log.Errorf("Error seeking the start of the file: %v", err)
		return err
	}

	// Read header stream bytes
	binaryHeader := make([]byte, headerSize)
	n, err := f.fileHeader.Read(binaryHeader)
	if err != nil {
		log.Errorf("Error reading the header: %v", err)
		return err
	}
	if n != headerSize {
		log.Error("Error getting header info")
		return ErrGettingHeaderInfo
	}

	// Convert to header struct
	f.mutexHeader.Lock()
	f.header, err = decodeBinaryToHeaderEntry(binaryHeader)
	f.writtenHead = f.header
	f.mutexHeader.Unlock()
	if err != nil {
		log.Error("Error decoding binary header")
		return err
	}

	return nil
}

// rollbackHeader cancels current file written entries not committed
func (f *StreamFile) rollbackHeader() error {
	// Restore header
	err := f.readHeaderEntry()
	if err != nil {
		return err
	}

	// Set file position to write
	_, err = f.file.Seek(int64(f.header.TotalLength), io.SeekStart)
	if err != nil {
		log.Errorf("Error seeking new position to write: %v", err)
		return err
	}

	return nil
}

// getHeaderEntry returns current committed header
func (f *StreamFile) getHeaderEntry() HeaderEntry {
	return f.writtenHead
}

// PrintHeaderEntry prints file header information
func PrintHeaderEntry(e HeaderEntry, title string) {
	log.Infof("--- HEADER ENTRY %s -------------------------", title)
	log.Infof("packetType: [%d]", e.packetType)
	log.Infof("headerLength: [%d]", e.headLength)
	log.Infof("Version: [%d]", e.Version)
	log.Infof("SystemID: [%d]", e.SystemID)
	log.Infof("streamType: [%d]", e.streamType)
	log.Infof("totalLength: [%d]", e.TotalLength)
	log.Infof("totalEntries: [%d]", e.TotalEntries)

	numPage := (e.TotalLength - PageHeaderSize) / PageDataSize
	offPage := (e.TotalLength - PageHeaderSize) % PageDataSize
	log.Infof("DataPage num=[%d] off=[%d]", numPage, offPage)
}

// writeHeaderEntry writes the memory header struct into the file header
func (f *StreamFile) writeHeaderEntry() error {
	// Position at the beginning of the file
	_, err := f.fileHeader.Seek(magicNumSize, io.SeekStart)
	if err != nil {
		log.Errorf("Error seeking the start of the file: %v", err)
		return err
	}

	// Write after convert header struct to binary stream
	binaryHeader := encodeHeaderEntryToBinary(f.header)
	log.Debugf("writing header entry: %v", binaryHeader)
	_, err = f.fileHeader.Write(binaryHeader)
	if err != nil {
		log.Errorf("Error writing the header: %v", err)
		return err
	}

	// Update the written header
	f.mutexHeader.Lock()
	f.writtenHead = f.header
	f.mutexHeader.Unlock()
	return nil
}

// encodeHeaderEntryToBinary encodes from a header entry type to binary bytes slice
func encodeHeaderEntryToBinary(e HeaderEntry) []byte {
	be := make([]byte, 1)
	be[0] = e.packetType
	be = binary.BigEndian.AppendUint32(be, e.headLength)
	be = append(be, e.Version)
	be = binary.BigEndian.AppendUint64(be, e.SystemID)
	be = binary.BigEndian.AppendUint64(be, uint64(e.streamType))
	be = binary.BigEndian.AppendUint64(be, e.TotalLength)
	be = binary.BigEndian.AppendUint64(be, e.TotalEntries)
	return be
}

// decodeBinaryToHeaderEntry decodes from binary bytes slice to a header entry type
func decodeBinaryToHeaderEntry(b []byte) (HeaderEntry, error) {
	e := HeaderEntry{}

	if len(b) != headerSize {
		log.Error("Invalid binary header entry")
		return e, ErrInvalidBinaryHeader
	}

	e.packetType = b[0]
	e.headLength = binary.BigEndian.Uint32(b[1:5])
	e.Version = b[5]
	e.SystemID = binary.BigEndian.Uint64(b[6:14])
	e.streamType = StreamType(binary.BigEndian.Uint64(b[14:22]))
	e.TotalLength = binary.BigEndian.Uint64(b[22:30])
	e.TotalEntries = binary.BigEndian.Uint64(b[30:38])

	return e, nil
}

// encodeFileEntryToBinary encodes from a data file entry type to binary bytes
func encodeFileEntryToBinary(e FileEntry) []byte {
	be := make([]byte, 1)
	be[0] = e.packetType
	be = binary.BigEndian.AppendUint32(be, e.Length)
	be = binary.BigEndian.AppendUint32(be, uint32(e.Type))
	be = binary.BigEndian.AppendUint64(be, e.Number)
	be = append(be, e.Data...)
	return be
}

// checkFileConsistency performs some file consistency checks
func (f *StreamFile) checkFileConsistency() error {
	// Get file info
	info, err := os.Stat(f.fileName)
	if err != nil {
		log.Error("Error checking file consistency")
		return err
	}

	// Check header page is present
	if info.Size() < PageHeaderSize {
		log.Error("Invalid file: missing header page")
		return ErrInvalidFileMissingHeaderPage
	}

	// Check data pages are not cut
	dataSize := info.Size() - PageHeaderSize
	uncut := dataSize % int64(f.pageSize)
	if uncut != 0 {
		log.Error("Inconsistent file size there is a cut data page")
		return ErrBadFileSizeCutDataPage
	}

	return nil
}

// checkMagicNumbers performs magic bytes check
func (f *StreamFile) checkMagicNumbers() error {
	// Position at the beginning of the file
	_, err := f.file.Seek(0, io.SeekStart)
	if err != nil {
		log.Errorf("Error seeking the start of the file: %v", err)
		return err
	}

	// Read magic numbers
	magic := make([]byte, magicNumSize)
	_, err = f.file.Read(magic)
	if err != nil {
		log.Errorf("Error reading magic numbers: %v", err)
		return err
	}

	// Check magic numbers
	if !bytes.Equal(magic, magicNumbers) {
		log.Errorf("Invalid magic numbers. Bad file?")
		return ErrBadFileFormat
	}

	return nil
}

// checkHeaderConsistency performs some header struct checks
func (f *StreamFile) checkHeaderConsistency() error {
	var err error = nil

	if f.header.packetType != PtHeader {
		log.Error("Invalid header: bad packet type")
		err = ErrInvalidHeaderBadPacketType
	} else if f.header.headLength != headerSize {
		log.Error("Invalid header: bad header length")
		err = ErrInvalidHeaderBadHeaderLength
	} else if f.header.streamType != f.streamType {
		log.Error("Invalid header: bad stream type")
		err = ErrInvalidHeaderBadStreamType
	}

	return err
}

// AddFileEntry writes new data entry to the data stream file
func (f *StreamFile) AddFileEntry(e FileEntry) error {
	var err error

	// Convert from data struct to bytes stream
	be := encodeFileEntryToBinary(e)

	// Check if the entry fits on current page
	var pageRemaining uint64
	entryLength := uint64(len(be))
	if (f.header.TotalLength-PageHeaderSize)%PageDataSize == 0 {
		pageRemaining = 0
	} else {
		pageRemaining = PageDataSize - (f.header.TotalLength-PageHeaderSize)%PageDataSize
	}
	if entryLength > pageRemaining {
		log.Debugf(">> Fill with pad entries. PageRemaining:%d, EntryLength:%d", pageRemaining, entryLength)
		err = f.fillPagePadEntries()
		if err != nil {
			return err
		}

		// Check if file is full
		if f.header.TotalLength == f.maxLength {
			// Add new data pages to the file
			log.Infof(">> FULL FILE (TotalLength: %d) -> extending!", f.header.TotalLength)
			err = f.extendFile()
			if err != nil {
				return err
			}

			log.Infof(">> New file max length: %d", f.maxLength)

			// Re-set the file position to write
			_, err = f.file.Seek(int64(f.header.TotalLength), io.SeekStart)
			if err != nil {
				log.Errorf("Error seeking position to write after file extend: %v", err)
				return err
			}
		}
	}

	// Write the data entry
	_, err = f.file.Write(be)
	if err != nil {
		log.Errorf("Error writing the entry: %v", err)
		return err
	}

	// Update the current header in memory (on disk later when the commit arrives)
	f.mutexHeader.Lock()
	f.header.TotalLength = f.header.TotalLength + entryLength
	f.header.TotalEntries = f.header.TotalEntries + 1
	f.mutexHeader.Unlock()

	// printHeaderEntry(f.header)
	return nil
}

// fillPagePadEntries fills remaining free space on the current data page with pad
func (f *StreamFile) fillPagePadEntries() error {
	// Page remaining free space
	var pageRemaining uint64
	if (f.header.TotalLength-PageHeaderSize)%PageDataSize == 0 {
		pageRemaining = 0
	} else {
		pageRemaining = PageDataSize - (f.header.TotalLength-PageHeaderSize)%PageDataSize
	}

	if pageRemaining > 0 {
		// Write pad entry
		_, err := f.file.Write([]byte{0})
		if err != nil {
			log.Errorf("Error writing pad entry: %v", err)
			return err
		}

		// Set the file position to write
		_, err = f.file.Seek(int64(pageRemaining-1), io.SeekCurrent)
		if err != nil {
			log.Errorf("Error seeking next write position after pad: %v", err)
			return err
		}

		// Update the current header in memory (on disk later when the commit arrives)
		f.mutexHeader.Lock()
		f.header.TotalLength = f.header.TotalLength + pageRemaining
		f.mutexHeader.Unlock()
	}

	return nil
}

// printStreamFile prints file information
func printStreamFile(f *StreamFile) {
	log.Info("--- STREAM FILE --------------------------")
	log.Infof("fileName: [%s]", f.fileName)
	log.Infof("pageSize: [%d]", f.pageSize)
	log.Infof("streamType: [%d]", f.streamType)
	log.Infof("maxLength: [%d]", f.maxLength)
	log.Infof("numDataPages=[%d]", (f.maxLength-PageHeaderSize)/PageDataSize)
	PrintHeaderEntry(f.header, "")
}

// DecodeBinaryToFileEntry decodes from binary bytes slice to file entry type
func DecodeBinaryToFileEntry(b []byte) (FileEntry, error) {
	d := FileEntry{}

	if len(b) < FixedSizeFileEntry {
		log.Error("Invalid binary data entry")
		return d, ErrInvalidBinaryEntry
	}

	d.packetType = b[0]
	d.Length = binary.BigEndian.Uint32(b[1:5])
	d.Type = EntryType(binary.BigEndian.Uint32(b[5:9]))
	d.Number = binary.BigEndian.Uint64(b[9:17])
	d.Data = b[17:]

	if uint32(len(d.Data)) != d.Length-FixedSizeFileEntry {
		log.Error("Error decoding binary data entry")
		return d, ErrDecodingBinaryDataEntry
	}

	return d, nil
}

// iteratorFrom initializes iterator to locate a data entry number in the stream file
func (f *StreamFile) iteratorFrom(entryNum uint64, readOnly bool) (*iteratorFile, error) {
	// Check starting entry number
	if entryNum >= f.writtenHead.TotalEntries {
		log.Infof("Invalid starting entry number for iterator")
		return nil, ErrInvalidEntryNumber
	}

	// Iterator mode
	var flag int
	if readOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_RDWR
	}

	// Open file for read only
	file, err := os.OpenFile(f.fileName, flag, os.ModePerm)
	if err != nil {
		log.Errorf("Error opening file for iterator: %v", err)
		return nil, err
	}

	// Create iterator struct
	iterator := iteratorFile{
		fromEntry: entryNum,
		file:      file,
		Entry: FileEntry{
			Number: 0,
		},
	}

	// Locate the file start stream point using custom dichotomic search
	err = f.seekEntry(&iterator)

	return &iterator, err
}

// iteratorNext gets the next data entry in the file for the iterator, returns the end of entries condition
func (f *StreamFile) iteratorNext(iterator *iteratorFile) (bool, error) {
	// Check end of entries condition
	if iterator.Entry.Number >= f.writtenHead.TotalEntries {
		return true, nil
	}

	// Read just the packet type
	packet := make([]byte, 1)
	_, err := iterator.file.Read(packet)
	if err != nil {
		log.Errorf("Error reading packet type for iterator: %v", err)
		return true, err
	}

	// Check if it is of type pad, if so forward to next data page on the file
	if packet[0] == PtPadding {
		// Current file position
		pos, err := iterator.file.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Errorf("Error seeking current pos for iterator: %v", err)
			return true, err
		}

		// Bytes to forward until next data page
		var forward int64
		if (pos-PageHeaderSize)%PageDataSize == 0 {
			forward = 0
		} else {
			forward = PageDataSize - ((pos - PageHeaderSize) % PageDataSize)
		}

		// Check end of data pages condition
		if pos+forward >= int64(f.writtenHead.TotalLength) {
			return true, nil
		}

		// Seek for the start of next data page
		_, err = iterator.file.Seek(forward, io.SeekCurrent)
		if err != nil {
			log.Errorf("Error seeking next page for iterator: %v", err)
			return true, err
		}

		// Read the new packet type
		_, err = iterator.file.Read(packet)
		if err != nil {
			log.Errorf("Error reading new packet type for iterator: %v", err)
			return true, err
		}
	}

	// Should be of type data
	if packet[0] != PtData {
		log.Errorf("Error expecting packet of type data(%d). Read: %d", PtData, packet[0])
		return true, ErrExpectingPacketTypeData
	}

	// Read the rest of fixed data entry bytes
	buffer := make([]byte, FixedSizeFileEntry-1)
	_, err = iterator.file.Read(buffer)
	if err != nil {
		log.Errorf("Error reading entry for iterator: %v", err)
		return true, err
	}
	buffer = append(packet, buffer...)

	// Check length
	length := binary.BigEndian.Uint32(buffer[1:5])
	if length < FixedSizeFileEntry {
		log.Errorf("Error decoding length data entry")
		err = ErrDecodingLengthDataEntry
		return true, err
	}

	// Read variable data
	if length > FixedSizeFileEntry {
		bufferAux := make([]byte, length-FixedSizeFileEntry)
		_, err = iterator.file.Read(bufferAux)
		if err != nil {
			log.Errorf("Error reading data for iterator: %v", err)
			return true, err
		}
		buffer = append(buffer, bufferAux...)
	}

	// Convert to data entry struct
	iterator.Entry, err = DecodeBinaryToFileEntry(buffer)
	if err != nil {
		log.Errorf("Error decoding entry for iterator: %v", err)
		return true, err
	}

	return false, nil
}

// iteratorEnd finalizes the file iterator
func (f *StreamFile) iteratorEnd(iterator *iteratorFile) {
	iterator.file.Close()
}

// seekEntry uses a file iterator to locate a data entry number using a custom binary search
func (f *StreamFile) seekEntry(iterator *iteratorFile) error {
	// Start and end data pages
	avg := 0
	beg := 0
	end := int((f.writtenHead.TotalLength - PageHeaderSize) / PageDataSize)
	if (f.writtenHead.TotalLength-PageHeaderSize)%PageDataSize == 0 {
		end = end - 1
	}

	// Custom binary search
	for beg <= end {
		avg = beg + (end-beg)/2 // nolint:gomnd

		// Seek for the start of avg data page
		newPos := (avg * PageDataSize) + PageHeaderSize
		_, err := iterator.file.Seek(int64(newPos), io.SeekStart)
		if err != nil {
			log.Errorf("Error seeking page for iterator seek entry: %v", err)
			return err
		}

		// Read fixed data entry bytes
		buffer := make([]byte, FixedSizeFileEntry)
		_, err = iterator.file.Read(buffer)
		if err != nil {
			log.Errorf("Error reading entry for iterator seek entry: %v", err)
			return err
		}

		// Decode packet type
		packetType := buffer[0]
		if packetType != PtData {
			log.Errorf("Error data page %d not starting with packet of type data. Type: %d", avg, packetType)
			return ErrPageNotStartingWithEntryData
		}

		// Decode entry number and compare it with the one we are looking for
		entryNum := binary.BigEndian.Uint64(buffer[9:17])
		if entryNum == iterator.fromEntry {
			// Found! the first of the page
			break
		} else if beg == end {
			// Should be found in this page
			err = f.locateEntry(iterator)
			if err != nil {
				return err
			}
			break
		} else if entryNum > iterator.fromEntry {
			// Bigger value, cut half the search pages
			end = avg - 1
		} else if entryNum < iterator.fromEntry {
			// Smaller value but could be inside the page, let's check it
			nextPageEntryNum, err := f.getFirstEntryOnNextPage(iterator)
			if err != nil {
				return err
			}

			// Smaller value committed, cut half the search pages
			if nextPageEntryNum <= iterator.fromEntry {
				beg = avg + 1
			} else {
				// First of next page is bigger so should be found in this page
				err = f.locateEntry(iterator)
				if err != nil {
					return err
				}
				break
			}
		}
	}

	// Back to the start of the data entry
	_, err := iterator.file.Seek(-FixedSizeFileEntry, io.SeekCurrent)
	if err != nil {
		log.Errorf("Error seeking page for iterator seek entry: %v", err)
		return err
	}

	log.Debugf("Entry number %d is in the data page %d", iterator.fromEntry, avg)
	return nil
}

// getFirstEntryOnNextPage returns the first data entry number on next page using an iterator
func (f *StreamFile) getFirstEntryOnNextPage(iterator *iteratorFile) (uint64, error) {
	// Current file position
	curpos, err := iterator.file.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("Error seeking current pos: %v", err)
		return 0, err
	}

	// Check if it is valid the current file position
	if curpos < PageHeaderSize || curpos > int64(f.writtenHead.TotalLength) {
		log.Errorf("Error current file position outside a data page")
		return 0, ErrCurrentPositionOutsideDataPage
	}

	// Check if exists another data page
	var forward int64
	if (curpos-PageHeaderSize)%PageDataSize == 0 {
		forward = 0
	} else {
		forward = PageDataSize - (curpos-PageHeaderSize)%PageDataSize
	}

	if curpos+forward >= int64(f.writtenHead.TotalLength) {
		return math.MaxUint64, nil
	}

	// Seek for the start of next data page
	_, err = iterator.file.Seek(int64(forward), io.SeekCurrent)
	if err != nil {
		log.Errorf("Error seeking next data page: %v", err)
		return 0, err
	}

	// Read fixed data entry bytes
	buffer := make([]byte, FixedSizeFileEntry)
	_, err = iterator.file.Read(buffer)
	if err != nil {
		log.Errorf("Error reading entry: %v", err)
		return 0, err
	}

	// Decode packet type
	if buffer[0] != PtData {
		log.Errorf("Error data page not starting with packet of type data(%d). Type: %d", PtData, buffer[0])
		return 0, ErrPageNotStartingWithEntryData
	}

	// Decode entry number
	entryNum := binary.BigEndian.Uint64(buffer[9:17])

	// Restore file position
	_, err = iterator.file.Seek(-int64(forward+FixedSizeFileEntry), io.SeekCurrent)
	if err != nil {
		log.Errorf("Error seeking current pos: %v", err)
		return 0, err
	}

	return entryNum, nil
}

// locateEntry locates the entry number we are looking for using the sequential iterator
func (f *StreamFile) locateEntry(iterator *iteratorFile) error {
	// Seek backward to the start of data entry
	_, err := iterator.file.Seek(-FixedSizeFileEntry, io.SeekCurrent)
	if err != nil {
		log.Errorf("Error in file seeking: %v", err)
		return err
	}

	for {
		end, err := f.iteratorNext(iterator)
		if err != nil {
			return err
		}

		// Not found
		if end || iterator.Entry.Number > iterator.fromEntry {
			log.Infof("Error can not locate the data entry number: %d", iterator.fromEntry)
			return ErrEntryNotFound
		}

		// Found!
		if iterator.Entry.Number == iterator.fromEntry {
			// Seek backward to the end of fixed data
			backward := iterator.Entry.Length - FixedSizeFileEntry
			_, err = iterator.file.Seek(-int64(backward), io.SeekCurrent)
			if err != nil {
				log.Errorf("Error in file seeking: %v", err)
				return err
			}
			break
		}
	}
	return nil
}

// updateEntryData updates the internal data of an entry in the file
func (f *StreamFile) updateEntryData(entryNum uint64, etype EntryType, data []byte) error {
	// Check the entry number
	if entryNum >= f.writtenHead.TotalEntries {
		log.Infof("Invalid entry number [%d], not committed in the file", entryNum)
		return ErrInvalidEntryNumberNotCommittedInFile
	}

	// Create iterator and locate the entry in the file
	iterator, err := f.iteratorFrom(entryNum, false)
	if err != nil {
		return err
	}

	// Get current entry data
	_, err = f.iteratorNext(iterator)
	if err != nil {
		return err
	}

	// Sanity check
	if iterator.Entry.Number != entryNum {
		log.Errorf("Entry number to update doesn't match. Current[%d] Update[%d]", iterator.Entry.Number, entryNum)
		return ErrEntryNumberMismatch
	}

	// Check entry type
	if iterator.Entry.Type != etype {
		log.Infof("Updating entry to a different entry type not allowed. Current[%d] Update[%d]", iterator.Entry.Type, etype)
		return ErrUpdateEntryTypeNotAllowed
	}

	// Check length of data
	dataLength := iterator.Entry.Length - FixedSizeFileEntry
	if dataLength != uint32(len(data)) {
		log.Infof("Updating entry data to a different length not allowed. Current[%d] Update[%d]", dataLength, uint32(len(data)))
		return ErrUpdateEntryDifferentSize
	}

	// Back to the start of the data in the file
	_, err = iterator.file.Seek(-int64(dataLength), io.SeekCurrent)
	if err != nil {
		log.Errorf("Error file seeking for update entry data: %v", err)
		return err
	}

	// Write new data entry
	_, err = iterator.file.Write(data)
	if err != nil {
		log.Errorf("Error writing updated entry data: %v", err)
		return err
	}

	// Flush data to disk
	err = iterator.file.Sync()
	if err != nil {
		log.Errorf("Error flushing updated entry data to disk: %v", err)
		return err
	}

	// Close iterator
	f.iteratorEnd(iterator)

	return nil
}

// truncateFile truncates file from an entry number onwards
func (f *StreamFile) truncateFile(entryNum uint64) error {
	// Create iterator and locate the entry in the file
	iterator, err := f.iteratorFrom(entryNum, true)
	if err != nil {
		return err
	}

	// Current file position
	curpos, err := iterator.file.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("Error seeking current pos: %v", err)
		return err
	}

	// Update internal header
	f.mutexHeader.Lock()
	f.header.TotalEntries = entryNum
	f.header.TotalLength = uint64(curpos)
	f.writtenHead = f.header
	f.mutexHeader.Unlock()

	// Write the header into the file (commit changes)
	err = f.writeHeaderEntry()
	if err != nil {
		return nil
	}

	// Set new file position to write
	_, err = f.file.Seek(int64(f.header.TotalLength), io.SeekStart)
	if err != nil {
		log.Errorf("Error seeking new position to write: %v", err)
		return err
	}

	return nil
}
