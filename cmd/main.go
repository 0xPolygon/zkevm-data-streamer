package main

import (
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/0xPolygonHermez/zkevm-data-streamer/tool/db"
)

const (
	// Entry types (events)
	EtStartL2Block datastreamer.EntryType = 1
	EtExecuteL2Tx  datastreamer.EntryType = 2

	StSequencer = 1
)

func main() {
	// Set log level
	log.Init(log.Config{
		Environment: "development",
		Level:       "debug",
		Outputs:     []string{"stdout"},
	})

	log.Info(">> App begin")

	// Create stream server
	s, err := datastreamer.New(1337, StSequencer, "streamfile.bin")
	if err != nil {
		os.Exit(1)
	}

	// Set data entries definition
	entriesDefinition := map[datastreamer.EntryType]datastreamer.EntityDefinition{
		EtStartL2Block: {
			Name:       "L2Block",
			StreamType: db.StreamTypeSequencer,
			Definition: reflect.TypeOf(db.L2Block{}),
		},
		EtExecuteL2Tx: {
			Name:       "L2Transaction",
			StreamType: db.StreamTypeSequencer,
			Definition: reflect.TypeOf(db.L2Transaction{}),
		},
	}
	s.SetEntriesDefinition(entriesDefinition)

	// Start stream server
	err = s.Start()
	if err != nil {
		log.Error(">> App error! Start")
		return
	}

	// Create and start clients
	go startNewClient()
	go startNewClient()

	// time.Sleep(2 * time.Second)

	// ------------------------------------------------------------
	// Fake Sequencer data
	l2tx := db.L2Transaction{
		BatchNumber:                 1,
		EffectiveGasPricePercentage: 255,
		IsValid:                     1,
		EncodedLength:               5,
		Encoded:                     []byte{1, 2, 3, 4, 5},
	}
	data := l2tx.Encode()

	go func() {
		for n := 1; n <= 2; n++ {
			// Start atomic operation
			err = s.StartAtomicOp()
			if err != nil {
				log.Error(">> App error! StartStreamTx")
				return
			}

			// Add stream entries
			for i := 1; i <= 5; i++ {
				_, err := s.AddStreamEntry(2, data)
				if err != nil {
					log.Errorf(">> App error! AddStreamEntry: %v", err)
					return
				}
			}

			// Commit atomic operation
			err = s.CommitAtomicOp()
			if err != nil {
				log.Error(">> App error! CommitStreamTx")
				return
			}

			time.Sleep(2 * time.Second)
		}
	}()
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
	c, err := datastreamer.NewClient("127.0.0.1:1337", StSequencer)
	if err != nil {
		return
	}

	// Start client (connect to the server)
	err = c.Start()
	if err != nil {
		return
	}

	// Get header status (execute command Header)
	err = c.ExecCommand(datastreamer.CmdHeader)
	if err != nil {
		return
	}

	// Start streaming receive (execute command Start)
	c.FromEntry = c.Header.TotalEntries
	err = c.ExecCommand(datastreamer.CmdStart)
	if err != nil {
		return
	}
}
