package datastreamer

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
)

type StreamClient struct {
	server     string
	streamType StreamType
	conn       net.Conn
	id         string
}

func NewClient(server string, streamType StreamType) (StreamClient, error) {
	// Create the client data stream
	c := StreamClient{
		server:     server,
		streamType: streamType,
		id:         "",
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
	return nil
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

	// Manage each command type
	err = c.manageCommand(cmd)
	if err != nil {
		return err
	}

	// Read server result entry for the command
	r, err := c.readResultEntry()
	if err != nil {
		log.Errorf("%s %v", c.id, err)
		return err
	}

	log.Infof("%s Result %d[%v] received for command %d", c.id, r.errorNum, r.errorStr, cmd)

	// Streaming receive goroutine
	if cmd == CmdStart {
		go c.streamingReceive()
	}

	return nil
}

func (c *StreamClient) manageCommand(cmd Command) error {
	var err error

	switch cmd {
	case CmdHeader:
	case CmdStart:
		// Send the starting entry number
		err = writeFullUint64(555, c.conn)
		if err != nil {
			log.Errorf("%s %v", c.id, err)
			return err
		}

	case CmdStop:
	default:
	}
	return nil
}

func (c *StreamClient) streamingReceive() {
	defer c.conn.Close()

	for {
		// Wait next data entry streamed
		d, err := c.readDataEntry()
		if err != nil {
			return
		}

		log.Debugf("(%s) %d | %d | %d | %d", c.id, d.packetType, d.length, d.entryType, d.entryNum)
	}
}

func (c *StreamClient) readDataEntry() (FileEntry, error) {
	d := FileEntry{}
	reader := bufio.NewReader(c.conn)

	// Read fixed size fields
	buffer := make([]byte, FixedSizeFileEntry)
	_, err := io.ReadFull(reader, buffer)
	if err != nil {
		if err == io.EOF {
			log.Errorf("%s Server close connection", c.id)
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
	_, err = io.ReadFull(reader, bufferAux)
	if err != nil {
		if err == io.EOF {
			log.Errorf("%s Server close connection", c.id)
		} else {
			log.Errorf("%s Error reading from server: %v", c.id, err)
		}
		return d, err
	}
	buffer = append(buffer, bufferAux...)

	// Decode binary data entry
	d, err = DecodeBinaryToFileEntry(buffer)
	if err != nil {
		return d, err
	}

	return d, nil
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
	reader := bufio.NewReader(c.conn)

	// Read fixed size fields
	buffer := make([]byte, FixedSizeResultEntry)
	_, err := io.ReadFull(reader, buffer)
	if err != nil {
		if err == io.EOF {
			log.Errorf("%s Server close connection", c.id)
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
	_, err = io.ReadFull(reader, bufferAux)
	if err != nil {
		if err == io.EOF {
			log.Errorf("%s Server close connection", c.id)
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
