package datastreamer

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
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
		fmt.Println("**Error connecting to server:", server, err)
		return
	}

	defer conn.Close()
	fmt.Println("**Connected to server:", server)

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
				fmt.Printf("client %s: no more data\n", client)
				return
			}
			fmt.Printf("client %s:error reading from server:%s\n", client, err)
			time.Sleep(2 * time.Second)
			continue
		}

		fmt.Printf("client %s:message from server:[%s]\n", client, buffer[:n])
	}
}

func writeFullUint64(value uint64, conn net.Conn) error {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, uint64(value))

	_, err := conn.Write(buffer)
	if err != nil {
		fmt.Println("**Error sending to server:", err)
		return err
	}
	return nil
}

func readResultEntry(conn net.Conn) (ResultEntry, error) {
	e := ResultEntry{}
	reader := bufio.NewReader(conn)

	// Read fixed fields (isentry 1byte, length 4bytes, errNum 4bytes)
	buffer := make([]byte, 9)
	_, err := io.ReadFull(reader, buffer)
	if err != nil {
		if err == io.EOF {
			fmt.Println("**Server close connection")
		} else {
			fmt.Println("**Error reading from server:", err)
		}
		return e, err
	}

	// Read variable field (errStr)
	length := binary.BigEndian.Uint32(buffer[1:5])
	if length < 10 {
		fmt.Println("**Error reading result entry")
		return e, errors.New("error reading result entry")
	}

	bufferAux := make([]byte, length-9)
	_, err = io.ReadFull(reader, bufferAux)
	if err != nil {
		if err == io.EOF {
			fmt.Println("**Server close connection")
		} else {
			fmt.Println("**Error reading from server:", err)
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
