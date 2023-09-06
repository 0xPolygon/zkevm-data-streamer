package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
)

func main() {
	log.Info(">> App begin")

	// Create server stream
	s, err := datastreamer.New(1337, "streamfile.bin")
	if err != nil {
		os.Exit(1)
	}
	err = s.Start()
	if err != nil {
		log.Error(">> App error! Start")
		return
	}

	// Create clients
	go startNewClient()
	go startNewClient()

	time.Sleep(2 * time.Second)

	// ------------------------------------------------------------
	// Fake Sequencer data
	data := make([]byte, 32)
	for i := 0; i < 32; i++ {
		data[i] = byte(i)
	}

	// Start atomic operation
	err = s.StartAtomicOp()
	if err != nil {
		log.Error(">> App error! StartStreamTx")
		return
	}

	// Add stream entries
	for i := 1; i <= 3; i++ {
		entry, err := s.AddStreamEntry(1, data)
		if err != nil {
			log.Error(">> App error! AddStreamEntry:", err)
			return
		}
		log.Info(">> App info. Added entry:", entry)
	}

	// Commit atomic operation
	err = s.CommitAtomicOp()
	if err != nil {
		log.Error(">> App error! CommitStreamTx")
		return
	}
	// ------------------------------------------------------------

	// Wait for ctl+c
	log.Info(">> Press Control+C to finish...")
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGTERM)
	<-interruptSignal

	log.Info(">> App end")
}

func startNewClient() {
	// Create client
	log.Debug("### New client")
	c, err := datastreamer.NewClient("127.0.0.1:1337")
	if err != nil {
		return
	}

	// Start client (connect to the server)
	log.Debug("### Start client")
	err = c.Start()
	if err != nil {
		return
	}

	// Start streaming receive (execute command Start)
	log.Debug("### Send Start command client")
	err = c.ExecCommand(datastreamer.CmdStart)
	if err != nil {
		return
	}
}
