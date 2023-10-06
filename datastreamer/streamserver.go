package datastreamer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"go.uber.org/zap/zapcore"
)

// Command type for the TCP client commands
type Command uint64

// ClientStatus type for the status of the client
type ClientStatus uint64

// AOStatus type for the atomic operation internal states
type AOStatus uint64

// EntryType type for the entry event types
type EntryType uint32

// StreamType type for the stream types
type StreamType uint64

// CommandError type for the command responses
type CommandError uint32

const (
	maxConnections = 100 // Maximum number of connected clients
	streamBuffer   = 128 // Buffers for the stream channel

	CmdStart  Command = 1 // CmdStart for start TCP client command
	CmdStop   Command = 2 // CmdStop for stop TCP client command
	CmdHeader Command = 3 // CmdHeader for header TCP client command

	CmdErrOK             CommandError = 0 // CmdErrOK for no error
	CmdErrAlreadyStarted CommandError = 1 // CmdErrAlreadyStarted for client already started error
	CmdErrAlreadyStopped CommandError = 2 // CmdErrAlreadyStopped for client already stopped error
	CmdErrBadFromEntry   CommandError = 3 // CmdErrBadFromEntry for invalid starting entry number
	CmdErrInvalidCommand CommandError = 9 // CmdErrInvalidCommand for invalid/unknown command error

	// Client status
	csSyncing ClientStatus = 1
	csSynced  ClientStatus = 2
	csStopped ClientStatus = 3
	csKilled  ClientStatus = 0xff

	// Atomic operation status
	aoNone        AOStatus = 1
	aoStarted     AOStatus = 2
	aoCommitting  AOStatus = 3
	aoRollbacking AOStatus = 4
)

var (
	// StrClientStatus for client status description
	StrClientStatus = map[ClientStatus]string{
		csSyncing: "Syncing",
		csSynced:  "Synced",
		csStopped: "Stopped",
		csKilled:  "Killed",
	}

	// StrCommand for TCP commands description
	StrCommand = map[Command]string{
		CmdStart:  "Start",
		CmdStop:   "Stop",
		CmdHeader: "Header",
	}

	// StrCommandErrors for TCP command errors description
	StrCommandErrors = map[CommandError]string{
		CmdErrOK:             "OK",
		CmdErrAlreadyStarted: "Already started",
		CmdErrAlreadyStopped: "Already stopped",
		CmdErrBadFromEntry:   "Bad from entry",
		CmdErrInvalidCommand: "Invalid command",
	}
)

// StreamServer type to manage a data stream server
type StreamServer struct {
	port     uint16 // Server stream port
	fileName string // Stream file name

	streamType StreamType
	ln         net.Listener
	clients    map[string]*client

	nextEntry  uint64        // Next sequential entry number
	atomicOp   streamAO      // Current in progress (if any) atomic operation
	stream     chan streamAO // Channel to stream committed atomic operations
	streamFile StreamFile
	bookmark   StreamBookmark

	entriesDef map[EntryType]EntityDefinition
}

// streamAO type to manage atomic operations
type streamAO struct {
	status     AOStatus
	startEntry uint64
	entries    []FileEntry
}

// client type for the server to manage clients
type client struct {
	conn   net.Conn
	status ClientStatus
}

// ResultEntry type for a result entry
type ResultEntry struct {
	packetType uint8 // 0xff:Result
	length     uint32
	errorNum   uint32 // 0:No error
	errorStr   []byte
}

// New creates a new data stream server
func New(port uint16, streamType StreamType, fileName string, cfg *log.Config) (StreamServer, error) {
	// Create the server data stream
	s := StreamServer{
		port:     port,
		fileName: fileName,

		streamType: streamType,
		ln:         nil,
		clients:    make(map[string]*client),
		nextEntry:  0,

		atomicOp: streamAO{
			status:     aoNone,
			startEntry: 0,
			entries:    []FileEntry{},
		},
		stream: make(chan streamAO, streamBuffer),
	}

	// Initialize the logger
	if cfg != nil {
		log.Init(*cfg)
	}

	// Open (or create) the data stream file
	var err error
	s.streamFile, err = PrepareStreamFile(s.fileName, s.streamType)
	if err != nil {
		return s, err
	}

	// Initialize the data entry number
	s.nextEntry = s.streamFile.header.TotalEntries

	// Open (or create) the bookmarks DB
	name := s.fileName[0:strings.IndexRune(s.fileName, '.')] + ".db"
	s.bookmark, err = PrepareBookmark(name)
	if err != nil {
		return s, err
	}

	return s, nil
}

// Start opens access to TCP clients and starts broadcasting
func (s *StreamServer) Start() error {
	// Start the server data stream
	var err error
	s.ln, err = net.Listen("tcp", ":"+strconv.Itoa(int(s.port)))
	if err != nil {
		log.Errorf("Error creating datastream server %d: %v", s.port, err)
		return err
	}

	// Goroutine to broadcast committed atomic operations
	go s.broadcastAtomicOp()

	// Goroutine to wait for clients connections
	log.Infof("Listening on port: %d", s.port)
	go s.waitConnections()

	return nil
}

// SetEntriesDef sets the event data fields definition
func (s *StreamServer) SetEntriesDef(entriesDef map[EntryType]EntityDefinition) {
	s.entriesDef = entriesDef
}

// waitConnections waits for a new client connection and creates a goroutine to manages it
func (s *StreamServer) waitConnections() {
	defer s.ln.Close()

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			log.Errorf("Error accepting new connection: %v", err)
			time.Sleep(2 * time.Second) // nolint:gomnd
			continue
		}

		// Check max connections allowed
		if len(s.clients) >= maxConnections {
			log.Warnf("Unable to accept client connection, maximum number of connections reached (%d)", maxConnections)
			conn.Close()
			time.Sleep(2 * time.Second) // nolint:gomnd
			continue
		}

		// Goroutine to manage client (command requests and entries stream)
		go s.handleConnection(conn)
	}
}

// handleConnection reads from the client connection and processes the received commands
func (s *StreamServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	clientId := conn.RemoteAddr().String()
	log.Debugf("New connection: %s", clientId)

	s.clients[clientId] = &client{
		conn:   conn,
		status: csStopped,
	}

	for {
		// Read command
		command, err := readFullUint64(conn)
		if err != nil {
			s.killClient(clientId)
			return
		}
		// Read stream type
		stUint64, err := readFullUint64(conn)
		if err != nil {
			s.killClient(clientId)
			return
		}
		st := StreamType(stUint64)

		// Check stream type
		if st != s.streamType {
			log.Errorf("Mismatch stream type, killed: %s", clientId)
			s.killClient(clientId)
			return
		}

		// Manage the requested command
		log.Infof("Command %d[%s] received from %s", command, StrCommand[Command(command)], clientId)
		err = s.processCommand(Command(command), clientId)
		if err != nil {
			// Kill client connection
			time.Sleep(1 * time.Second)
			s.killClient(clientId)
			return
		}
	}
}

// StartAtomicOp starts a new atomic operation
func (s *StreamServer) StartAtomicOp() error {
	start := time.Now().UnixNano()
	defer log.Infof("StartAtomicOp process time: %vns", time.Now().UnixNano()-start)

	log.Infof("!!!Start AtomicOp (%d)", s.nextEntry)
	if s.atomicOp.status == aoStarted {
		log.Errorf("AtomicOp already started and in progress after entry %d", s.atomicOp.startEntry)
		return errors.New("start not allowed, atomicop already started")
	}

	s.atomicOp.status = aoStarted
	s.atomicOp.startEntry = s.nextEntry
	return nil
}

// AddStreamEntry adds a new entry in the current atomic operation
func (s *StreamServer) AddStreamEntry(etype EntryType, data []byte) (uint64, error) {
	start := time.Now().UnixNano()
	defer log.Infof("AddStreamEntry process time: %vns", time.Now().UnixNano()-start)

	// Add to the stream file
	entryNum, err := s.addStream("Data", etype, data)

	return entryNum, err
}

// AddStreamBookmark adds a new bookmark in the current atomic operation
func (s *StreamServer) AddStreamBookmark(bookmark []byte) (uint64, error) {
	start := time.Now().UnixNano()
	defer log.Infof("AddStreamBookmark process time: %vns", time.Now().UnixNano()-start)

	// Add to the stream file
	entryNum, err := s.addStream("Bookmark", EtBookmark, bookmark)
	if err != nil {
		return 0, err
	}

	// Add to the bookmark index
	err = s.bookmark.AddBookmark(bookmark, entryNum)
	if err != nil {
		return entryNum, err
	}

	return entryNum, nil
}

// addStream adds a new stream entry in the current atomic operation
func (s *StreamServer) addStream(desc string, etype EntryType, data []byte) (uint64, error) {
	// Check atomic operation status
	if s.atomicOp.status != aoStarted {
		log.Errorf("Add stream entry not allowed, AtomicOp is not started")
		return 0, errors.New("add stream entry not allowed atomicop is not started")
	}

	// Generate data entry
	e := FileEntry{
		packetType: PtData,
		Length:     1 + 4 + 4 + 8 + uint32(len(data)),
		EntryType:  etype,
		EntryNum:   s.nextEntry,
		Data:       data,
	}

	// Log data entry fields
	if etype != EtBookmark && log.GetLevel() == zapcore.DebugLevel && e.packetType == PtData {
		entity := s.entriesDef[etype]
		if entity.Name != "" {
			log.Debugf("%s entry: %d | %d | %d | %d | %s", desc, e.EntryNum, e.packetType, e.Length, e.EntryType, entity.toString(data))
		} else {
			log.Warnf("%s entry: %d | %d | %d | %d | No definition for this entry type", desc, e.EntryNum, e.packetType, e.Length, e.EntryType)
		}
	} else {
		log.Infof("%s entry: %d | %d | %d | %d | %d", desc, e.EntryNum, e.packetType, e.Length, e.EntryType, len(data))
	}

	// Update header (in memory) and write data entry into the file
	err := s.streamFile.AddFileEntry(e)
	if err != nil {
		return 0, nil
	}

	// Save the entry in the atomic operation in progress
	s.atomicOp.entries = append(s.atomicOp.entries, e)

	// Increase sequential entry number
	s.nextEntry++

	return e.EntryNum, nil
}

// CommitAtomicOp commits the current atomic operation and streams it to the clients
func (s *StreamServer) CommitAtomicOp() error {
	start := time.Now().UnixNano()
	defer log.Infof("CommitAtomicOp process time: %vns", time.Now().UnixNano()-start)

	log.Infof("!!!Commit AtomicOp (%d)", s.atomicOp.startEntry)
	if s.atomicOp.status != aoStarted {
		log.Errorf("Commit not allowed, AtomicOp is not in the started state")
		return errors.New("commit not allowed, atomicop not in started state")
	}

	s.atomicOp.status = aoCommitting

	// Update header into the file (commit the new entries)
	err := s.streamFile.writeHeaderEntry()
	if err != nil {
		return err
	}

	// Do broadcast of the committed atomic operation to the stream clients
	atomic := streamAO{
		status:     s.atomicOp.status,
		startEntry: s.atomicOp.startEntry,
	}
	atomic.entries = make([]FileEntry, len(s.atomicOp.entries))
	copy(atomic.entries, s.atomicOp.entries)

	s.stream <- atomic

	// No atomic operation in progress
	s.clearAtomicOp()

	return nil
}

// RollbackAtomicOp cancels the current atomic operation and rollbacks the changes
func (s *StreamServer) RollbackAtomicOp() error {
	start := time.Now().UnixNano()
	defer log.Infof("RollbackAtomicOp process time: %vns", time.Now().UnixNano()-start)

	log.Infof("!!!Rollback AtomicOp (%d)", s.atomicOp.startEntry)
	if s.atomicOp.status != aoStarted {
		log.Errorf("Rollback not allowed, AtomicOp is not in the started state")
		return errors.New("rollback not allowed, atomicop not in the started state")
	}

	s.atomicOp.status = aoRollbacking

	// Restore header in memory (discard current) from the file header (rollback entries)
	err := s.streamFile.readHeaderEntry()
	if err != nil {
		return err
	}

	// Rollback the entry number
	s.nextEntry = s.atomicOp.startEntry

	// No atomic operation in progress
	s.clearAtomicOp()

	return nil
}

// GetHeader returns the current committed header
func (s *StreamServer) GetHeader() HeaderEntry {
	// Get current file header
	header := s.streamFile.getHeaderEntry()
	return header
}

// GetEntry searches in the stream file and returns the data for the requested entry
func (s *StreamServer) GetEntry(entryNum uint64) (FileEntry, error) {
	// Initialize file stream iterator
	iterator, err := s.streamFile.iteratorFrom(entryNum)
	if err != nil {
		return FileEntry{}, err
	}

	// Get requested entry data
	_, err = s.streamFile.iteratorNext(iterator)
	if err != nil {
		return FileEntry{}, err
	}

	// Close iterator
	s.streamFile.iteratorEnd(iterator)

	return iterator.Entry, nil
}

// clearAtomicOp sets the current atomic operation to none
func (s *StreamServer) clearAtomicOp() {
	// No atomic operation in progress and empty entries slice
	s.atomicOp.entries = s.atomicOp.entries[:0]
	s.atomicOp.status = aoNone
}

// broadcastAtomicOp broadcasts committed atomic operations to the clients
func (s *StreamServer) broadcastAtomicOp() {
	defer s.streamFile.file.Close()
	defer s.bookmark.db.Close()

	var err error
	for {
		// Wait for new atomic operation to broadcast
		broadcastOp := <-s.stream
		start := time.Now().UnixMilli()

		// For each connected and started client
		log.Infof("STREAM clients: %d, AtomicOP entries: %d", len(s.clients), len(broadcastOp.entries))
		for id, cli := range s.clients {
			log.Infof("Stream client %s status %d[%s]", id, cli.status, StrClientStatus[cli.status])
			if cli.status != csSynced {
				continue
			}

			// Send entries
			for _, entry := range broadcastOp.entries {
				log.Debugf("Sending data entry %d (type %d) to %s", entry.EntryNum, entry.EntryType, id)
				binaryEntry := encodeFileEntryToBinary(entry)

				// Send the file data entry
				if cli.conn != nil {
					_, err = cli.conn.Write(binaryEntry)
				} else {
					err = errors.New("error nil connection")
				}
				if err != nil {
					// Kill client connection
					log.Warnf("Error sending entry to %s: %v", id, err)
					s.killClient(id)
				}
			}
		}
		log.Infof("broadcastAtomicOp process time: %vms", time.Now().UnixMilli()-start)
	}
}

// killClient disconnects the client and removes it from server clients struct
func (s *StreamServer) killClient(clientId string) {
	if s.clients[clientId] != nil {
		if s.clients[clientId].status != csKilled {
			s.clients[clientId].status = csKilled
			if s.clients[clientId].conn != nil {
				s.clients[clientId].conn.Close()
			}
			delete(s.clients, clientId)
		}
	}
}

// processCommand manages the received TCP commands from the clients
func (s *StreamServer) processCommand(command Command, clientId string) error {
	cli := s.clients[clientId]

	// Manage each different kind of command request from a client
	var err error
	switch command {
	case CmdStart:
		if cli.status != csStopped {
			log.Error("Stream to client already started!")
			err = errors.New("client already started")
			_ = s.sendResultEntry(uint32(CmdErrAlreadyStarted), StrCommandErrors[CmdErrAlreadyStarted], clientId)
		} else {
			cli.status = csSyncing
			err = s.processCmdStart(clientId)
			if err == nil {
				cli.status = csSynced
			}
		}

	case CmdStop:
		if cli.status != csSynced {
			log.Error("Stream to client already stopped!")
			err = errors.New("client already stopped")
			_ = s.sendResultEntry(uint32(CmdErrAlreadyStopped), StrCommandErrors[CmdErrAlreadyStopped], clientId)
		} else {
			cli.status = csStopped
			err = s.processCmdStop(clientId)
		}

	case CmdHeader:
		if cli.status != csStopped {
			log.Error("Header command not allowed, stream started!")
			err = errors.New("header command not allowed")
			_ = s.sendResultEntry(uint32(CmdErrAlreadyStarted), StrCommandErrors[CmdErrAlreadyStarted], clientId)
		} else {
			err = s.processCmdHeader(clientId)
		}

	default:
		log.Error("Invalid command!")
		err = errors.New("invalid command")
		_ = s.sendResultEntry(uint32(CmdErrInvalidCommand), StrCommandErrors[CmdErrInvalidCommand], clientId)
	}

	return err
}

// processCmdStart processes the TCP Start command from the clients
func (s *StreamServer) processCmdStart(clientId string) error {
	// Read from entry number parameter
	conn := s.clients[clientId].conn
	fromEntry, err := readFullUint64(conn)
	if err != nil {
		return err
	}

	// Log
	log.Infof("Client %s command Start from %d", clientId, fromEntry)

	// Check received param
	if fromEntry > s.nextEntry {
		log.Errorf("Start command invalid from entry %d for client %s", fromEntry, clientId)
		err = errors.New("start command invalid param from entry")
		_ = s.sendResultEntry(uint32(CmdErrBadFromEntry), StrCommandErrors[CmdErrBadFromEntry], clientId)
		return err
	}

	// Send a command result entry OK
	err = s.sendResultEntry(0, "OK", clientId)
	if err != nil {
		return err
	}

	// Stream entries data from the requested entry number
	if fromEntry < s.nextEntry {
		err = s.streamingFromEntry(clientId, fromEntry)
	}

	return err
}

// processCmdStop processes the TCP Stop command from the clients
func (s *StreamServer) processCmdStop(clientId string) error {
	// Log
	log.Infof("Client %s command Stop", clientId)

	// Send a command result entry OK
	err := s.sendResultEntry(0, "OK", clientId)
	return err
}

// processCmdHeader processes the TCP Header command from the clients
func (s *StreamServer) processCmdHeader(clientId string) error {
	// Log
	log.Infof("Client %s command Header", clientId)

	// Send a command result entry OK
	err := s.sendResultEntry(0, "OK", clientId)
	if err != nil {
		return err
	}

	// Get current written/committed file header
	header := s.streamFile.getHeaderEntry()
	binaryHeader := encodeHeaderEntryToBinary(header)

	// Send header entry to the client
	conn := s.clients[clientId].conn
	if conn != nil {
		_, err = conn.Write(binaryHeader)
	} else {
		err = errors.New("error nil connection")
	}
	if err != nil {
		log.Warnf("Error sending header entry to %s: %v", clientId, err)
		return err
	}
	return nil
}

// streamingFromEntry sends to the client the stream data starting from the requested entry number
func (s *StreamServer) streamingFromEntry(clientId string, fromEntry uint64) error {
	// Log
	conn := s.clients[clientId].conn
	log.Infof("SYNCING %s from entry %d...", clientId, fromEntry)

	// Start file stream iterator
	iterator, err := s.streamFile.iteratorFrom(fromEntry)
	if err != nil {
		return err
	}

	// Loop data entries from file stream iterator
	for {
		end, err := s.streamFile.iteratorNext(iterator)
		if err != nil {
			return err
		}

		// Check if end of iterator
		if end {
			break
		}

		// Send the file data entry
		binaryEntry := encodeFileEntryToBinary(iterator.Entry)
		log.Infof("Sending data entry %d (type %d) to %s", iterator.Entry.EntryNum, iterator.Entry.EntryType, clientId)
		if conn != nil {
			_, err = conn.Write(binaryEntry)
		} else {
			err = errors.New("error nil connection")
		}
		if err != nil {
			log.Warnf("Error sending entry %d to %s: %v", iterator.Entry.EntryNum, clientId, err)
			return err
		}
	}
	log.Infof("Synced %s until %d!", clientId, iterator.Entry.EntryNum)

	// Close iterator
	s.streamFile.iteratorEnd(iterator)

	return nil
}

// sendResultEntry sends the response to a TCP command for the clients
func (s *StreamServer) sendResultEntry(errorNum uint32, errorStr string, clientId string) error {
	// Prepare the result entry
	byteSlice := []byte(errorStr)

	entry := ResultEntry{
		packetType: PtResult,
		length:     1 + 4 + 4 + uint32(len(byteSlice)),
		errorNum:   errorNum,
		errorStr:   byteSlice,
	}
	// PrintResultEntry(entry) // TODO: remove

	// Convert struct to binary bytes
	binaryEntry := encodeResultEntryToBinary(entry)
	log.Debugf("result entry: %v", binaryEntry)

	// Send the result entry to the client
	var err error
	conn := s.clients[clientId].conn
	if conn != nil {
		_, err = conn.Write(binaryEntry)
	} else {
		err = errors.New("error nil connection")
	}
	if err != nil {
		log.Warnf("Error sending result entry to %s: %v", clientId, err)
		return err
	}
	return nil
}

// readFullUint64 reads from a connection a complete uint64
func readFullUint64(conn net.Conn) (uint64, error) {
	// Read 8 bytes (uint64 value)
	buffer := make([]byte, 8) // nolint:gomnd
	n, err := io.ReadFull(conn, buffer)
	if err != nil {
		if err == io.EOF {
			log.Debugf("Client %s close connection", conn.RemoteAddr().String())
		} else {
			log.Warnf("Error reading from client: %v", err)
		}
		return 0, err
	}

	// Convert bytes to uint64
	var value uint64
	err = binary.Read(bytes.NewReader(buffer[:n]), binary.BigEndian, &value)
	if err != nil {
		log.Error("Error converting bytes to uint64")
		return 0, err
	}

	return value, nil
}

// encodeResultEntryToBinary encodes from a result entry type to binary bytes slice
func encodeResultEntryToBinary(e ResultEntry) []byte {
	be := make([]byte, 1)
	be[0] = e.packetType
	be = binary.BigEndian.AppendUint32(be, e.length)
	be = binary.BigEndian.AppendUint32(be, e.errorNum)
	be = append(be, e.errorStr...)
	return be
}

// DecodeBinaryToResultEntry decodes from binary bytes slice to a result entry type
func DecodeBinaryToResultEntry(b []byte) (ResultEntry, error) {
	e := ResultEntry{}

	if len(b) < FixedSizeResultEntry {
		log.Error("Invalid binary result entry")
		return e, errors.New("invalid binary result entry")
	}

	e.packetType = b[0]
	e.length = binary.BigEndian.Uint32(b[1:5])
	e.errorNum = binary.BigEndian.Uint32(b[5:9])
	e.errorStr = b[9:]

	if uint32(len(e.errorStr)) != e.length-FixedSizeResultEntry {
		log.Error("Error decoding binary result entry")
		return e, errors.New("error decoding binary result entry")
	}

	return e, nil
}

// PrintResultEntry prints result entry type
func PrintResultEntry(e ResultEntry) {
	log.Debug("--- RESULT ENTRY -------------------------")
	log.Debugf("packetType: [%d]", e.packetType)
	log.Debugf("length: [%d]", e.length)
	log.Debugf("errorNum: [%d]", e.errorNum)
	log.Debugf("errorStr: [%s]", e.errorStr)
}
