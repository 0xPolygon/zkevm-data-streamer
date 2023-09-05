package main

import (
	"os"
	"os/signal"
	"syscall"

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
	go datastreamer.NewClient(1)

	// Fake stream data
	data := make([]byte, 32)
	for i := 0; i < 32; i++ {
		data[i] = byte(i)
	}

	// Start tx
	err = s.StartStreamTx()
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

	// Commit tx
	err = s.CommitStreamTx()
	if err != nil {
		log.Error(">> App error! CommitStreamTx")
		return
	}

	// Wait for ctl+c
	log.Info(">> Press Control+C to finish...")
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGTERM)
	<-interruptSignal

	log.Info(">> App end")
}
