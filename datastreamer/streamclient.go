package datastreamer

import (
	"encoding/binary"
	"errors"
	"io"
	"net"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"go.uber.org/zap/zapcore"
)

const (
	resultsBuffer = 32  // Buffers for the results channel
	headersBuffer = 32  // Buffers for the headers channel
	entriesBuffer = 128 // Buffers for the entries channel
)

// ProcessEntryFunc type of the callback function to process the received entry
type ProcessEntryFunc func(*FileEntry, *StreamClient, *StreamServer) error

// StreamClient type to manage a data stream client
type StreamClient struct {
	server     string // Server address to connect IP:port
	streamType StreamType
	conn       net.Conn
	Id         string // Client id

	FromEntry    uint64      // Set starting entry number for the Start command
	FromBookmark []byte      // Set starting bookmark for the StartBookmark command
	Header       HeaderEntry // Header info received from the Header command

	results chan ResultEntry // Channel to read command results
	headers chan HeaderEntry // Channel to read header entries from the command Header
	entries chan FileEntry   // Channel to read data entries from the streaming

	processEntry ProcessEntryFunc // Callback function to process the entry
	relayServer  *StreamServer    // Only used by the client on the stream relay server

	entriesDef map[EntryType]EntityDefinition
}

// NewClient creates a new data stream client
func NewClient(server string, streamType StreamType) (StreamClient, error) {
	// Create the client data stream
	c := StreamClient{
		server:     server,
		streamType: streamType,
		Id:         "",
		FromEntry:  0,

		results: make(chan ResultEntry, resultsBuffer),
		headers: make(chan HeaderEntry, headersBuffer),
		entries: make(chan FileEntry, entriesBuffer),

		relayServer: nil,
	}

	// Set default callback function to process entry
	c.SetProcessEntryFunc(PrintReceivedEntry, c.relayServer)

	return c, nil
}

// Start connects to the data stream server and starts getting data from the server
func (c *StreamClient) Start() error {
	// Connect to server
	var err error
	c.conn, err = net.Dial("tcp", c.server)
	if err != nil {
		log.Errorf("Error connecting to server %s: %v", c.server, err)
		return err
	}

	c.Id = c.conn.LocalAddr().String()
	log.Infof("%s Connected to server: %s", c.Id, c.server)

	// Goroutine to read from the server all entry types
	go c.readEntries()

	// Goroutine to consume streaming entries
	go c.getStreaming()

	return nil
}

// SetEntriesDef sets the event data fields definition
func (c *StreamClient) SetEntriesDef(entriesDef map[EntryType]EntityDefinition) {
	c.entriesDef = entriesDef
}

// GetEntryDef returns the definition of an entry type
func (c *StreamClient) GetEntryDef(etype EntryType) EntityDefinition {
	return c.entriesDef[etype]
}

// ExecCommand executes a valid client TCP command
func (c *StreamClient) ExecCommand(cmd Command) error {
	log.Infof("%s Executing command %d[%s]...", c.Id, cmd, StrCommand[cmd])

	// Check valid command
	if cmd != CmdStart && cmd != CmdStartBookmark && cmd != CmdHeader && cmd != CmdStop {
		log.Errorf("%s Invalid command %d", c.Id, cmd)
		return errors.New("invalid command")
	}

	// Send command
	err := writeFullUint64(uint64(cmd), c.conn)
	if err != nil {
		return err
	}
	// Send stream type
	err = writeFullUint64(uint64(c.streamType), c.conn)
	if err != nil {
		return err
	}

	// Send the Start and StartBookmark parameters
	if cmd == CmdStart {
		log.Infof("%s ...from entry %d", c.Id, c.FromEntry)
		// Send starting/from entry number
		err = writeFullUint64(c.FromEntry, c.conn)
		if err != nil {
			return err
		}
	} else if cmd == CmdStartBookmark {
		log.Infof("%s ...from bookmark [%v]", c.Id, c.FromBookmark)
		// Send starting/from bookmark length
		err = writeFullUint32(uint32(len(c.FromBookmark)), c.conn)
		if err != nil {
			return err
		}
		// Send starting/from bookmark
		err = writeFullBytes(c.FromBookmark, c.conn)
		if err != nil {
			return err
		}
	}

	// Get command result
	r := c.getResult(cmd)
	if r.errorNum != uint32(CmdErrOK) {
		return errors.New("result command error")
	}

	// Get header entry
	if cmd == CmdHeader {
		h := c.getHeader()
		c.Header = h
	}

	return nil
}

// writeFullUint64 writes to connection a complete uint64
func writeFullUint64(value uint64, conn net.Conn) error {
	buffer := make([]byte, 8) // nolint:gomnd
	binary.BigEndian.PutUint64(buffer, value)

	var err error
	if conn != nil {
		_, err = conn.Write(buffer)
	} else {
		err = errors.New("error nil connection")
	}
	if err != nil {
		log.Errorf("%s Error sending to server: %v", conn.RemoteAddr().String(), err)
		return err
	}
	return nil
}

// writeFullUint32 writes to connection a complete uint32
func writeFullUint32(value uint32, conn net.Conn) error {
	buffer := make([]byte, 4) // nolint:gomnd
	binary.BigEndian.PutUint32(buffer, value)

	var err error
	if conn != nil {
		_, err = conn.Write(buffer)
	} else {
		err = errors.New("error nil connection")
	}
	if err != nil {
		log.Errorf("%s Error sending to server: %v", conn.RemoteAddr().String(), err)
		return err
	}
	return nil
}

// writeFullBytes writes to connection the complete buffer
func writeFullBytes(buffer []byte, conn net.Conn) error {
	var err error
	if conn != nil {
		_, err = conn.Write(buffer)
	} else {
		err = errors.New("error nil connection")
	}
	if err != nil {
		log.Errorf("%s Error sending to server: %v", conn.RemoteAddr().String(), err)
		return err
	}
	return nil
}

// readDataEntry reads bytes from server connection and returns a data entry type
func (c *StreamClient) readDataEntry() (FileEntry, error) {
	d := FileEntry{}

	// Read the rest of fixed size fields
	buffer := make([]byte, FixedSizeFileEntry-1)
	_, err := io.ReadFull(c.conn, buffer)
	if err != nil {
		if err == io.EOF {
			log.Warnf("%s Server close connection", c.Id)
		} else {
			log.Errorf("%s Error reading from server: %v", c.Id, err)
		}
		return d, err
	}
	packet := []byte{PtData}
	buffer = append(packet, buffer...)

	// Read variable field (data)
	length := binary.BigEndian.Uint32(buffer[1:5])
	if length < FixedSizeFileEntry {
		log.Errorf("%s Error reading data entry", c.Id)
		return d, errors.New("error reading data entry")
	}

	bufferAux := make([]byte, length-FixedSizeFileEntry)
	_, err = io.ReadFull(c.conn, bufferAux)
	if err != nil {
		if err == io.EOF {
			log.Warnf("%s Server close connection", c.Id)
		} else {
			log.Errorf("%s Error reading from server: %v", c.Id, err)
		}
		return d, err
	}
	buffer = append(buffer, bufferAux...)

	// Decode binary data to data entry struct
	d, err = DecodeBinaryToFileEntry(buffer)
	if err != nil {
		return d, err
	}

	return d, nil
}

// readHeaderEntry reads bytes from server connection and returns a header entry type
func (c *StreamClient) readHeaderEntry() (HeaderEntry, error) {
	h := HeaderEntry{}

	// Read the rest of header bytes
	buffer := make([]byte, headerSize-1)
	n, err := io.ReadFull(c.conn, buffer)
	if err != nil {
		log.Errorf("Error reading the header: %v", err)
		return h, err
	}
	if n != headerSize-1 {
		log.Error("Error getting header info")
		return h, errors.New("error getting header info")
	}
	packet := []byte{PtHeader}
	buffer = append(packet, buffer...)

	// Decode bytes stream to header entry struct
	h, err = decodeBinaryToHeaderEntry(buffer)
	if err != nil {
		log.Error("Error decoding binary header")
		return h, err
	}

	return h, nil
}

// readResultEntry reads bytes from server connection and returns a result entry type
func (c *StreamClient) readResultEntry() (ResultEntry, error) {
	e := ResultEntry{}

	// Read the rest of fixed size fields
	buffer := make([]byte, FixedSizeResultEntry-1)
	_, err := io.ReadFull(c.conn, buffer)
	if err != nil {
		if err == io.EOF {
			log.Warnf("%s Server close connection", c.Id)
		} else {
			log.Errorf("%s Error reading from server: %v", c.Id, err)
		}
		return e, err
	}
	packet := []byte{PtResult}
	buffer = append(packet, buffer...)

	// Read variable field (errStr)
	length := binary.BigEndian.Uint32(buffer[1:5])
	if length < FixedSizeResultEntry {
		log.Errorf("%s Error reading result entry", c.Id)
		return e, errors.New("error reading result entry")
	}

	bufferAux := make([]byte, length-FixedSizeResultEntry)
	_, err = io.ReadFull(c.conn, bufferAux)
	if err != nil {
		if err == io.EOF {
			log.Warnf("%s Server close connection", c.Id)
		} else {
			log.Errorf("%s Error reading from server: %v", c.Id, err)
		}
		return e, err
	}
	buffer = append(buffer, bufferAux...)

	// Decode binary entry result
	e, err = DecodeBinaryToResultEntry(buffer)
	if err != nil {
		return e, err
	}
	// PrintResultEntry(e)
	return e, nil
}

// readEntries reads from the server all type of packets
func (c *StreamClient) readEntries() {
	defer c.conn.Close()

	for {
		// Read packet type
		packet := make([]byte, 1)
		_, err := io.ReadFull(c.conn, packet)
		if err != nil {
			if err == io.EOF {
				log.Warnf("%s Server close connection", c.Id)
			} else {
				log.Errorf("%s Error reading from server: %v", c.Id, err)
			}
			return
		}

		// Manage packet type
		switch packet[0] {
		case PtResult:
			// Read result entry data
			r, err := c.readResultEntry()
			if err != nil {
				return
			}
			// Send data to results channel
			c.results <- r

		case PtHeader:
			// Read header entry data
			h, err := c.readHeaderEntry()
			if err != nil {
				return
			}
			// Send data to headers channel
			c.headers <- h

		case PtData:
			// Read file/stream entry data
			e, err := c.readDataEntry()
			if err != nil {
				return
			}
			// Send data to stream entries channel
			c.entries <- e

		default:
			// Unknown type
			log.Warnf("%s Unknown packet type %d", c.Id, packet[0])
			continue
		}
	}
}

// getResult consumes a result entry
func (c *StreamClient) getResult(cmd Command) ResultEntry {
	// Get result entry
	r := <-c.results
	log.Infof("%s Result %d[%s] received for command %d[%s]", c.Id, r.errorNum, r.errorStr, cmd, StrCommand[cmd])
	return r
}

// getHeader consumes a header entry
func (c *StreamClient) getHeader() HeaderEntry {
	h := <-c.headers
	log.Infof("%s Header received info: TotalEntries[%d], TotalLength[%d]", c.Id, h.TotalEntries, h.TotalLength)
	return h
}

// getStreaming consumes streaming data entries
func (c *StreamClient) getStreaming() {
	for {
		e := <-c.entries

		// Process the data entry
		err := c.processEntry(&e, c, c.relayServer)
		if err != nil {
			log.Errorf("%s Error processing entry %d", c.Id, e.Number)
		}
	}
}

// SetProcessEntryFunc sets the callback function to process entry
func (c *StreamClient) SetProcessEntryFunc(f ProcessEntryFunc, s *StreamServer) {
	c.processEntry = f
	c.relayServer = s
}

// PrintReceivedEntry prints received entry (default callback function)
func PrintReceivedEntry(e *FileEntry, c *StreamClient, s *StreamServer) error {
	// Log data entry fields
	if e.Type != EtBookmark && log.GetLevel() == zapcore.DebugLevel {
		entity := c.GetEntryDef(e.Type)
		if entity.Name != "" {
			log.Debugf("Data entry(%s): %d | %d | %d | %s", c.Id, e.Number, e.Length, e.Type, entity.ToString(e.Data))
		} else {
			log.Warnf("Data entry(%s): %d | %d | %d | No definition for this entry type", c.Id, e.Number, e.Length, e.Type)
		}
	} else {
		log.Infof("Data entry(%s): %d | %d | %d | %d", c.Id, e.Number, e.Length, e.Type, len(e.Data))
	}

	return nil
}
