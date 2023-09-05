package datastreamer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
)

type Command uint64
type ClientStatus uint64
type TxStatus uint64

const (
	// Stream type
	StSequencer = 1 // Sequencer

	// Commands
	CmdStart  Command = 1
	CmdStop   Command = 2
	CmdHeader Command = 3

	// Client status
	csStarted ClientStatus = 1
	csStopped ClientStatus = 2

	// Transaction status
	txNone       TxStatus = 0
	txStarted    TxStatus = 1
	txCommitting TxStatus = 2
)

type StreamServer struct {
	port     uint16 // server stream port
	fileName string // stream file name

	streamType uint64
	ln         net.Listener
	clients    map[string]streamClient

	lastEntry uint64
	tx        streamTx
	fs        StreamFile
}

type streamTx struct {
	status     TxStatus
	afterEntry uint64
}

type streamClient struct {
	conn   net.Conn
	status ClientStatus
}

type ResultEntry struct {
	isEntry  uint8 // 0xff: Result
	length   uint32
	errorNum uint32 // 0:No error
	errorStr []byte
}

func New(port uint16, fileName string) (StreamServer, error) {
	// Create the server data stream
	s := StreamServer{
		port:     port,
		fileName: fileName,

		streamType: StSequencer,
		ln:         nil,
		clients:    make(map[string]streamClient),
		lastEntry:  0,

		tx: streamTx{
			status:     txNone,
			afterEntry: 0,
		},
	}

	// Open (or create) the data stream file
	var err error
	s.fs, err = PrepareStreamFile(s.fileName, s.streamType)
	if err != nil {
		return s, err
	}

	// Initialize the entry number
	s.lastEntry = s.fs.header.totalEntries

	return s, nil
}

func (s *StreamServer) Start() error {
	// Start the server data stream
	var err error
	s.ln, err = net.Listen("tcp", ":"+strconv.Itoa(int(s.port)))
	if err != nil {
		log.Error("Error creating datastream server:", s.port, err)
		return err
	}

	// Wait for clients connections
	log.Info("Listening on port:", s.port)
	go s.waitConnections()

	return nil
}

func (s *StreamServer) waitConnections() {
	defer s.ln.Close()

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			log.Error("Error accepting new connection:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		// Goroutine to manage client (command requests and entries stream)
		go s.handleConnection(conn)
	}
}

func (s *StreamServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	clientId := conn.RemoteAddr().String()
	log.Info("New connection:", conn.RemoteAddr())

	client := streamClient{
		conn:   conn,
		status: csStopped,
	}

	s.clients[clientId] = client

	reader := bufio.NewReader(conn)
	for {
		// Read command and stream type
		command, err := readFullUint64(reader)
		if err != nil {
			return //TODO
		}
		st, err := readFullUint64(reader)
		if err != nil {
			return //TODO
		}
		if st != s.streamType {
			log.Error("Mismatch stream type, killed:", clientId)
			return //TODO
		}
		// Manage the requested command
		log.Infof("Command %d received from %s", command, clientId)
		err = s.processCommand(Command(command), clientId)
		if err != nil {
			// Kill client connection
			return
		}
	}
}

func (s *StreamServer) StartStreamTx() error {
	log.Debug("!!!Start Tx")
	s.tx.status = txStarted
	s.tx.afterEntry = s.lastEntry
	return nil
}

func (s *StreamServer) AddStreamEntry(etype uint32, data []uint8) (uint64, error) {
	log.Debug("!!!Add entry")
	e := FileEntry{
		isEntry:   IEEntry,
		length:    1 + 4 + 4 + 8 + uint32(len(data)),
		entryType: etype,
		entryNum:  s.lastEntry + 1,
		data:      data,
	}

	err := s.fs.AddFileEntry(e)
	if err != nil {
		return 0, nil
	}

	s.lastEntry++
	return s.lastEntry, nil
}

func (s *StreamServer) CommitStreamTx() error {
	log.Debug("!!!Commit Tx")
	s.tx.status = txCommitting
	// TODO: work
	err := s.fs.writeHeaderEntry()
	if err != nil {
		return err
	}

	s.tx.status = txNone
	return nil
}

func (s *StreamServer) processCommand(command Command, clientId string) error {
	client := s.clients[clientId]

	var err error = nil
	var errNum uint32 = 0

	// Manage each different kind of command request from a client
	switch command {
	case CmdStart:
		if client.status != csStopped {
			log.Error("Stream to client already started!")
			err = errors.New("client already started")
		} else {
			client.status = csStarted
			// TODO
		}

	case CmdStop:
		if client.status != csStarted {
			log.Error("Stream to client already stopped!")
			err = errors.New("client already stopped")
		} else {
			client.status = csStopped
			// TODO
		}

	case CmdHeader:
		if client.status != csStopped {
			log.Error("Header command not allowed, stream started!")
			err = errors.New("header command not allowed")
		}

	default:
		log.Error("Invalid command!")
		err = errors.New("invalid command")
	}

	var errStr string
	if err != nil {
		errStr = err.Error()
	} else {
		errStr = "OK"
	}
	err = s.sendResultEntry(errNum, errStr, clientId)
	return err
}

// Send the response to a command that is a result entry
func (s *StreamServer) sendResultEntry(errorNum uint32, errorStr string, clientId string) error {
	// Prepare the result entry
	byteSlice := []byte(errorStr)

	entry := ResultEntry{
		isEntry:  0xff,
		length:   1 + 4 + 4 + uint32(len(byteSlice)),
		errorNum: errorNum,
		errorStr: byteSlice,
	}
	PrintResultEntry(entry) // TODO: remove

	// Convert struct to binary bytes
	binaryEntry := encodeResultEntryToBinary(entry)
	log.Debug("result entry:", binaryEntry)

	// Send the result entry to the client
	conn := s.clients[clientId].conn
	writer := bufio.NewWriter(conn)
	_, err := writer.Write(binaryEntry)
	if err != nil {
		log.Error("Error sending result entry")
	}
	writer.Flush()

	return nil
}

func readFullUint64(reader *bufio.Reader) (uint64, error) {
	// Read 8 bytes (uint64 value)
	buffer := make([]byte, 8)
	n, err := io.ReadFull(reader, buffer)
	if err != nil {
		if err == io.EOF {
			log.Warn("Client close connection")
		} else {
			log.Error("Error reading from client:", err)
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

// Encode/convert from an entry type to binary bytes slice
func encodeResultEntryToBinary(e ResultEntry) []byte {
	be := make([]byte, 1)
	be[0] = e.isEntry
	be = binary.BigEndian.AppendUint32(be, e.length)
	be = binary.BigEndian.AppendUint32(be, e.errorNum)
	be = append(be, e.errorStr...)
	return be
}

// Decode/convert from binary bytes slice to an entry type
func DecodeBinaryToResultEntry(b []byte) (ResultEntry, error) {
	e := ResultEntry{}

	if len(b) < 10 {
		log.Error("Invalid binary result entry")
		return e, errors.New("invalid binary result entry")
	}

	e.isEntry = b[0]
	e.length = binary.BigEndian.Uint32(b[1:5])
	e.errorNum = binary.BigEndian.Uint32(b[5:9])
	e.errorStr = b[9:]

	if uint32(len(e.errorStr)) != e.length-1-4-4 {
		log.Error("Error decoding binary result entry")
		return e, errors.New("error decoding binary result entry")
	}

	return e, nil
}

func PrintResultEntry(e ResultEntry) {
	log.Debug("--- RESULT ENTRY -------------------------")
	log.Debugf("isEntry: [%d]", e.isEntry)
	log.Debugf("length: [%d]", e.length)
	log.Debugf("errorNum: [%d]", e.errorNum)
	log.Debugf("errorStr: [%s]", e.errorStr)
}
