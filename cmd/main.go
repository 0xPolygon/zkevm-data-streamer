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
	ss, err := datastreamer.New(1337, "streamfile.bin")
	if err != nil {
		os.Exit(1)
	}
	err = ss.Start()
	if err != nil {
		os.Exit(1)
	}

	// Create clients
	go datastreamer.NewClient(1)

	// Wait for ctl+c
	fmt.Println(">> Press Control+C to finish...")
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGTERM)
	<-interruptSignal

	fmt.Println(">> App end")
}
