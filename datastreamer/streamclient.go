package datastreamer

import (
	"encoding/binary"
	"errors"
	"io"
	"net"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
)

type StreamClient struct {
	server     string // Server address to connect IP:port
	streamType StreamType
	conn       net.Conn
	id         string // Client id

	FromEntry uint64      // Set starting entry data (for Start command)
	Header    HeaderEntry // Header info received (from Header command)

	entriesDefinition map[EntryType]EntityDefinition
}

func NewClient(server string, streamType StreamType) (StreamClient, error) {
	// Create the client data stream
	c := StreamClient{
		server:     server,
		streamType: streamType,
		id:         "",
		FromEntry:  0,
	}
	return c, nil
}

func (c *StreamClient) Start() error {
	// Connect to server
	var err error
	c.conn, err = net.Dial("tcp", c.server)
	if err != nil {
		log.Errorf("Error connecting to server %s: %v", c.server, err)
		return err
	}

	c.id = c.conn.LocalAddr().String()
	log.Infof("%s Connected to server: %s", c.id, c.server)

	// Receiving stream
	// go c.receivingStreaming()

	return nil
}

// func (c *StreamClient) receivingStreaming() {
// 	defer c.conn.Close()
// 	c.streamingRead()
// }

func (c *StreamClient) SetEntriesDefinition(entriesDef map[EntryType]EntityDefinition) {
	c.entriesDefinition = entriesDef
}

func (c *StreamClient) ExecCommand(cmd Command) error {
	// Send command
	err := writeFullUint64(uint64(cmd), c.conn)
	if err != nil {
		log.Errorf("%s %v", c.id, err)
		return err
	}
	// Send stream type
	err = writeFullUint64(uint64(c.streamType), c.conn)
	if err != nil {
		log.Errorf("%s %v", c.id, err)
		return err
	}

	// Send the Start command parameter
	if cmd == CmdStart {
		// Send starting/from entry number
		err = writeFullUint64(c.FromEntry, c.conn)
		if err != nil {
			log.Errorf("%s %v", c.id, err)
			return err
		}
	}

	// Read server result entry for the command
	r, err := c.readResultEntry()
	if err != nil {
		log.Errorf("%s %v", c.id, err)
		return err
	}
	log.Infof("%s Result %d[%s] received for command %d[%s]", c.id, r.errorNum, r.errorStr, cmd, StrCommand[cmd])

	// Manage each command type
	err = c.manageCommand(cmd)
	if err != nil {
		return err
	}

	return nil
}

func (c *StreamClient) manageCommand(cmd Command) error {
	switch cmd {
	case CmdHeader:
		// Read header entry
		h, err := c.readHeaderEntry()
		if err == nil {
			c.Header = h
		}

	case CmdStart:
		// Streaming receive goroutine
		c.streamingRead() // TODO: work / call as goroutine?

	case CmdStop:

	default:
		return errors.New("unknown command")
	}
	return nil
}

func (c *StreamClient) streamingRead() {
	defer c.conn.Close()
	for {
		// Wait next data entry streamed
		_, err := c.readDataEntry()
		if err != nil {
			return
		}
	}
}

func (c *StreamClient) readDataEntry() (FileEntry, error) {
	d := FileEntry{}

	// Read fixed size fields
	buffer := make([]byte, FixedSizeFileEntry)
	_, err := io.ReadFull(c.conn, buffer)
	if err != nil {
		if err == io.EOF {
			log.Warnf("%s Server close connection", c.id)
		} else {
			log.Errorf("%s Error reading from server: %v", c.id, err)
		}
		return d, err
	}

	// Read variable field (data)
	length := binary.BigEndian.Uint32(buffer[1:5])
	if length < FixedSizeFileEntry {
		log.Errorf("%s Error reading data entry", c.id)
		return d, errors.New("error reading data entry")
	}

	bufferAux := make([]byte, length-FixedSizeFileEntry)
	_, err = io.ReadFull(c.conn, bufferAux)
	if err != nil {
		if err == io.EOF {
			log.Warnf("%s Server close connection", c.id)
		} else {
			log.Errorf("%s Error reading from server: %v", c.id, err)
		}
		return d, err
	}
	buffer = append(buffer, bufferAux...)

	// Decode binary data to data entry struct
	d, err = DecodeBinaryToFileEntry(buffer)
	if err != nil {
		return d, err
	}

	// Log data entry fields
	if d.packetType == PtData {
		entity := c.entriesDefinition[d.entryType]
		if entity.Name != "" {
			log.Infof("Data entry (%s) %d|%d|%d|%d| %s", c.id, d.packetType, d.length, d.entryType, d.entryNum, entity.toString(d.data))
		} else {
			log.Warnf("Data entry (%s) %d|%d|%d|%d| No definition for this entry type", c.id, d.packetType, d.length, d.entryType, d.entryNum)
		}
	}

	return d, nil
}

func (c *StreamClient) readHeaderEntry() (HeaderEntry, error) {
	h := HeaderEntry{}

	// Read header stream bytes
	binaryHeader := make([]byte, headerSize)
	n, err := io.ReadFull(c.conn, binaryHeader)
	if err != nil {
		log.Errorf("Error reading the header: %v", err)
		return h, err
	}
	if n != headerSize {
		log.Error("Error getting header info")
		return h, errors.New("error getting header info")
	}

	// Decode bytes stream to header entry struct
	h, err = decodeBinaryToHeaderEntry(binaryHeader)
	if err != nil {
		log.Error("Error decoding binary header")
		return h, err
	}

	return h, nil
}

func writeFullUint64(value uint64, conn net.Conn) error {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, uint64(value))

	_, err := conn.Write(buffer)
	if err != nil {
		log.Errorf("%s Error sending to server: %v", conn.RemoteAddr().String(), err)
		return err
	}
	return nil
}

func (c *StreamClient) readResultEntry() (ResultEntry, error) {
	e := ResultEntry{}

	// Read fixed size fields
	buffer := make([]byte, FixedSizeResultEntry)
	_, err := io.ReadFull(c.conn, buffer)
	if err != nil {
		if err == io.EOF {
			log.Warnf("%s Server close connection", c.id)
		} else {
			log.Errorf("%s Error reading from server: %v", c.id, err)
		}
		return e, err
	}

	// Read variable field (errStr)
	length := binary.BigEndian.Uint32(buffer[1:5])
	if length < FixedSizeResultEntry {
		log.Errorf("%s Error reading result entry", c.id)
		return e, errors.New("error reading result entry")
	}

	bufferAux := make([]byte, length-FixedSizeResultEntry)
	_, err = io.ReadFull(c.conn, bufferAux)
	if err != nil {
		if err == io.EOF {
			log.Warnf("%s Server close connection", c.id)
		} else {
			log.Errorf("%s Error reading from server: %v", c.id, err)
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
