package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
)

func main() {
	fmt.Println(">> App begin")

	// Create server stream
	s, err := datastreamer.New(1337, "streamfile.bin")
	if err != nil {
		os.Exit(1)
	}
	s.Start()

	// Create clients
	go datastreamer.NewClient(1)

	// Fake stream data
	data := make([]byte, 32)
	for i := 0; i < 32; i++ {
		data[i] = byte(i)
	}

	// Start tx
	s.StartStreamTx()

	// Add stream entries
	s.AddStreamEntry(1, data)
	s.AddStreamEntry(1, data)

	// Commit tx
	s.CommitStreamTx()

	// Wait for ctl+c
	fmt.Println(">> Press Control+C to finish...")
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGTERM)
	<-interruptSignal

	fmt.Println(">> App end")
}
