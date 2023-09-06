package datastreamer

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
)

const (
	server = "127.0.0.1:1337"
)

// For Development
func NewClient(i int) {
	// Connect to server
	conn, err := net.Dial("tcp", server)
	if err != nil {
		log.Error("**Error connecting to server:", server, err)
		return
	}

	defer conn.Close()
	log.Info("**Connected to server:", server)

	// Send the command and stream type
	err = writeFullUint64(uint64(i), conn)
	if err != nil {
		log.Error(err)
		return
	}
	err = writeFullUint64(StSequencer, conn)
	if err != nil {
		log.Error(err)
		return
	}

	// Read server result entry for the command
	_, err = readResultEntry(conn)
	if err != nil {
		log.Error(err)
		return
	}

	// Read from server
	readFromServer(conn)
}

func readFromServer(conn net.Conn) {
	client := conn.LocalAddr().String()
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				log.Errorf("**client %s: no more data", client)
				return
			}
			log.Errorf("**client %s:error reading from server:%s", client, err)
			time.Sleep(2 * time.Second)
			continue
		}

		log.Infof("**client %s:message from server:[%s]", client, buffer[:n])
	}
}

func writeFullUint64(value uint64, conn net.Conn) error {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, uint64(value))

	_, err := conn.Write(buffer)
	if err != nil {
		log.Error("**Error sending to server:", err)
		return err
	}
	return nil
}

func readResultEntry(conn net.Conn) (ResultEntry, error) {
	e := ResultEntry{}
	reader := bufio.NewReader(conn)

	// Read fixed fields (packetType 1byte, length 4bytes, errNum 4bytes)
	buffer := make([]byte, 9)
	_, err := io.ReadFull(reader, buffer)
	if err != nil {
		if err == io.EOF {
			log.Warn("**Server close connection")
		} else {
			log.Error("**Error reading from server:", err)
		}
		return e, err
	}

	// Read variable field (errStr)
	length := binary.BigEndian.Uint32(buffer[1:5])
	if length < 10 {
		log.Error("**Error reading result entry")
		return e, errors.New("error reading result entry")
	}

	bufferAux := make([]byte, length-9)
	_, err = io.ReadFull(reader, bufferAux)
	if err != nil {
		if err == io.EOF {
			log.Warn("**Server close connection")
		} else {
			log.Error("**Error reading from server:", err)
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
