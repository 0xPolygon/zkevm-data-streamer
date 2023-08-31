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
	pageSize  = 1024 * 1024 // 1 MB
	initPages = 7           // Initial number of data pages (not counting header page)
	nextPages = 8           // Number of data pages to add when run out
)

type HeaderEntry struct {
	packetType   uint8
	headLength   uint32
	streamType   uint64
	totalLength  uint64
	totalEntries uint64
}

type FileEntry struct {
	isEntry        uint8  // 0:Padding, 1:Header, 2:Entry
	length         uint32 // Length of the entry
	entryType      uint32 // 1:Tx, 2:Batch-start
	sequenceNumber uint64 // Entry sequential number (starts with 0)
	data           []byte
}

type FileStream struct {
	fileName string
	pageSize uint32 // in bytes
	file     *os.File

	header HeaderEntry

	numPages uint64
}

func PrepareStreamFile(fn string, st uint64) (FileStream, error) {
	fs := FileStream{
		fileName: fn,
		pageSize: pageSize,
		file:     nil,

		header: HeaderEntry{
			packetType:   ptSequencer,
			headLength:   29,
			streamType:   st,
			totalLength:  0,
			totalEntries: 0,
		},

		numPages: 0,
	}

	// Open (or create) the data stream file
	err := fs.openCreateFile()

	return fs, err
}

func (f *FileStream) openCreateFile() error {
	// Check if file exists (otherwise create it)
	_, err := os.Stat(f.fileName)

	if os.IsNotExist(err) {
		// File does not exists so create it
		fmt.Println("Creating file for datastrem:", f.fileName)
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
	fmt.Println("Number of pages:", f.numPages)

	return err
}

func (f *FileStream) initializeFile() error {
	// Create the header page
	err := f.createHeaderPage()
	if err != nil {
		return err
	}

	// Create initial data pages
	for i := 1; i <= initPages; i++ {
		err = f.createPage()
		if err != nil {
			fmt.Println("Eror creating page:", f.numPages+1)
			return err
		}
	}

	return err
}

func (f *FileStream) createHeaderPage() error {
	// Create the header page (first page) of the file
	err := f.createPage()
	if err != nil {
		fmt.Println("Error creating the header page:", err)
		return err
	}

	// Update the header entry
	err = f.writeHeaderEntry()
	return err
}

// Create/add a new page on the stream file
func (f *FileStream) createPage() error {
	page := make([]byte, f.pageSize)

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

	f.numPages++
	return nil
}

func (f *FileStream) writeHeaderEntry() error {
	_, err := f.file.Seek(0, 0)
	if err != nil {
		fmt.Println("Error seeking the start of the file:", err)
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

func (f *FileStream) checkFileConsistency() error {
	info, err := os.Stat(f.fileName)
	if err != nil {
		fmt.Println("Error checking file consistency")
		return err
	}

	uncut := info.Size() % int64(f.pageSize)
	if uncut != 0 {
		fmt.Println("Inconsistent file size there is a cut page")
		return errors.New("bad file size cut page")
	}

	f.numPages = uint64(info.Size()) / uint64(f.pageSize)
	return nil
}
