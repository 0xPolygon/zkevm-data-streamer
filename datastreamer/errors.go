package datastreamer

import "fmt"

var (
	// ErrInvalidCommand is returned when the command is invalid
	ErrInvalidCommand = fmt.Errorf("invalid command")
	// ErrResultCommandError is returned when the command is invalid
	ErrResultCommandError = fmt.Errorf("result command error")
	// ErrNilConnection is returned when the connection is nil
	ErrNilConnection = fmt.Errorf("nil connection")
	// ErrReadingDataEntry is returned when there is an error reading data entry
	ErrReadingDataEntry = fmt.Errorf("error reading data entry")
	// ErrReadingResultEntry is returned when there is an error reading result entry
	ErrReadingResultEntry = fmt.Errorf("error reading result entry")
	// ErrGettingHeaderInfo is returned when there is an error getting header info
	ErrGettingHeaderInfo = fmt.Errorf("error getting header info")
	// ErrInvalidBinaryHeader is returned when the binary header is invalid
	ErrInvalidBinaryHeader = fmt.Errorf("invalid binary header info")
	// ErrInvalidFileMissingHeaderPage is returned when the file is invalid, missing header page
	ErrInvalidFileMissingHeaderPage = fmt.Errorf("invalid file, missing header page")
	// ErrBadFileSizeCutDataPage is returned when the file size is bad, cut data page
	ErrBadFileSizeCutDataPage = fmt.Errorf("bad file size, cut data page")
	// ErrBadFileFormat is returned when the file format is bad
	ErrBadFileFormat = fmt.Errorf("bad file format")
	// ErrInvalidHeaderBadPacketType is returned when the header is invalid, bad packet type
	ErrInvalidHeaderBadPacketType = fmt.Errorf("invalid header, bad packet type")
	// ErrInvalidHeaderBadHeaderLength is returned when the header is invalid, bad header length
	ErrInvalidHeaderBadHeaderLength = fmt.Errorf("invalid header, bad header length")
	// ErrInvalidHeaderBadStreamType is returned when the header is invalid, bad stream type
	ErrInvalidHeaderBadStreamType = fmt.Errorf("invalid header, bad stream type")
	// ErrInvalidBinaryEntry is returned when the binary entry is invalid
	ErrInvalidBinaryEntry = fmt.Errorf("invalid binary entry")
	// ErrDecodingBinaryDataEntry is returned when there is an error decoding binary data entry
	ErrDecodingBinaryDataEntry = fmt.Errorf("error decoding binary data entry")
	// ErrExpectingPacketTypeData is returned when there is an error expecting packet type data
	ErrExpectingPacketTypeData = fmt.Errorf("expecting packet type data")
	// ErrDecodingLengthDataEntry is returned when there is an error decoding length data entry
	ErrDecodingLengthDataEntry = fmt.Errorf("error decoding length data entry")
	// ErrPageNotStartingWithEntryData is returned when the page is not starting with entry data
	ErrPageNotStartingWithEntryData = fmt.Errorf("page not starting with entry data")
	// ErrCurrentPositionOutsideDataPage is returned when the current position is outside data page
	ErrCurrentPositionOutsideDataPage = fmt.Errorf("current position outside data page")
	// ErrEntryNotFound is returned when the entry is not found
	ErrEntryNotFound = fmt.Errorf("entry not found")
	// ErrInvalidEntryNumberNotCommittedInFile is returned when the entry number is invalid, not committed in the file
	ErrInvalidEntryNumberNotCommittedInFile = fmt.Errorf("invalid entry number, not committed in the file")
	// ErrEntryNumberMismatch is returned when the entry number doesn't match
	ErrEntryNumberMismatch = fmt.Errorf("entry number doesn't match")
	// ErrUpdateEntryTypeNotAllowed is returned when the update entry type is not allowed
	ErrUpdateEntryTypeNotAllowed = fmt.Errorf("update entry to a different entry type not allowed")
	// ErrUpdateEntryDifferentSize is returned when the update entry is a different size
	ErrUpdateEntryDifferentSize = fmt.Errorf("update entry to a different size not allowed")
	// ErrAtomicOpNotAllowed is returned when the atomic operation is not allowed
	ErrAtomicOpNotAllowed = fmt.Errorf("atomicop not allowed, server is not started")
	// ErrStartAtomicOpNotAllowed is returned when the start atomic operation is not allowed
	ErrStartAtomicOpNotAllowed = fmt.Errorf("start atomicop not allowed, atomicop already started")
	// ErrAddEntryNotAllowed is returned when the add entry is not allowed
	ErrAddEntryNotAllowed = fmt.Errorf("add entry not allowed, atomicop is not started")
	// ErrCommitNotAllowed is returned when the commit is not allowed
	ErrCommitNotAllowed = fmt.Errorf("commit not allowed, atomicop not in started state")
	// ErrRollbackNotAllowed is returned when the rollback is not allowed
	ErrRollbackNotAllowed = fmt.Errorf("rollback not allowed, atomicop not in started state")
	// ErrInvalidEntryNumber is returned when the entry number is invalid
	ErrInvalidEntryNumber = fmt.Errorf("invalid entry number, doesn't exist")
	// ErrUpdateNotAllowed is returned when the update is not allowed
	ErrUpdateNotAllowed = fmt.Errorf("update not allowed, it's in current atomic operation")
	// ErrClientAlreadyStarted is returned when the client is already started
	ErrClientAlreadyStarted = fmt.Errorf("client already started")
	// ErrClientAlreadyStopped is returned when the client is already stopped
	ErrClientAlreadyStopped = fmt.Errorf("client already stopped")
	// ErrHeaderCommandNotAllowed is returned when the header command is not allowed
	ErrHeaderCommandNotAllowed = fmt.Errorf("header command not allowed")
	// ErrEntryCommandNotAllowed is returned when the entry command is not allowed
	ErrEntryCommandNotAllowed = fmt.Errorf("entry command not allowed")
	// ErrStartCommandInvalidParamFromEntry is returned when the start command is invalid, param from entry
	ErrStartCommandInvalidParamFromEntry = fmt.Errorf("start command invalid param from entry")
	// ErrStartBookmarkInvalidParamFromBookmark is returned when the start bookmark is invalid, param from bookmark
	ErrStartBookmarkInvalidParamFromBookmark = fmt.Errorf("start bookmark invalid param from bookmark")
	// ErrEndBookmarkInvalidParamToBookmark is returned when the end bookmark is invalid, param to bookmark
	ErrEndBookmarkInvalidParamToBookmark = fmt.Errorf("end bookmark invalid param to bookmark")
	// ErrInvalidBinaryResultEntry is returned when the binary result entry is invalid
	ErrInvalidBinaryResultEntry = fmt.Errorf("invalid binary result entry")
	// ErrDecodingBinaryResultEntry is returned when there is an error decoding binary result entry
	ErrDecodingBinaryResultEntry = fmt.Errorf("error decoding binary result entry")
	// ErrTruncateNotAllowed is returned when there is an atomic operation in progress
	ErrTruncateNotAllowed = fmt.Errorf("truncate not allowed, atomic operation in progress")
	// ErrBookmarkCommandNotAllowed is returned when the bookmark command is not allowed
	ErrBookmarkCommandNotAllowed = fmt.Errorf("bookmark command not allowed")
	// ErrExecCommandNotAllowed is returned when execute TCP command is not allowed
	ErrExecCommandNotAllowed = fmt.Errorf("execute command not allowed, client is not started")
	// ErrBookmarkNotFound is returned when the bookmark is not found
	ErrBookmarkNotFound = fmt.Errorf("bookmark not found")
	// ErrBookmarkMaxLength is returned when the bookmark length exceeds maximum length
	ErrBookmarkMaxLength = fmt.Errorf("bookmark max length")
	// ErrInvalidBookmarkRange is returned when the bookmark range is invalid
	ErrInvalidBookmarkRange = fmt.Errorf("invalid bookmark range")
)
