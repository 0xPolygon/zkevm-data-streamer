package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/ethereum/go-ethereum/common"
	"github.com/urfave/cli/v2"
)

const (
	EtL2BlockStart datastreamer.EntryType = 1 // EtL2BlockStart entry type
	EtL2Tx         datastreamer.EntryType = 2 // EtL2Tx entry type
	EtL2BlockEnd   datastreamer.EntryType = 3 // EtL2BlockEnd entry type

	StSequencer = 1 // StSequencer sequencer stream type
)

// main runs a datastream server or client
func main() {
	// Set log level
	log.Init(log.Config{
		Environment: "development",
		Level:       "info",
		Outputs:     []string{"stdout"},
	})

	app := cli.NewApp()

	app.Commands = []*cli.Command{
		{
			Name:    "server",
			Aliases: []string{"v"},
			Usage:   "Run the server",
			Action:  runServer,
		},
		{
			Name:    "client",
			Aliases: []string{},
			Usage:   "Run the client",
			Action:  runClient,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}

// runServer runs a local datastream server and tests its features
func runServer(*cli.Context) error {
	log.Info(">> App begin")

	// Create stream server
	s, err := datastreamer.New(6900, StSequencer, "datastream.bin", nil) // nolint:gomnd
	if err != nil {
		os.Exit(1)
	}

	// Set data entries definition
	entriesDefinition := map[datastreamer.EntryType]datastreamer.EntityDefinition{
		EtL2BlockStart: {
			Name:       "L2BlockStart",
			StreamType: datastreamer.StreamTypeSequencer,
			Definition: reflect.TypeOf(datastreamer.L2BlockStart{}),
		},
		EtL2Tx: {
			Name:       "L2Transaction",
			StreamType: datastreamer.StreamTypeSequencer,
			Definition: reflect.TypeOf(datastreamer.L2Transaction{}),
		},
		EtL2BlockEnd: {
			Name:       "L2BlockEnd",
			StreamType: datastreamer.StreamTypeSequencer,
			Definition: reflect.TypeOf(datastreamer.L2BlockEnd{}),
		},
	}
	s.SetEntriesDef(entriesDefinition)

	// Start stream server
	err = s.Start()
	if err != nil {
		log.Error(">> App error! Start")
		return err
	}

	// s.BookmarkPrintDump()
	// time.Sleep(5 * time.Second) // nolint:gomnd

	// ------------------------------------------------------------
	// Fake Sequencer data
	l2blockStart := datastreamer.L2BlockStart{
		BatchNumber:    101,               // nolint:gomnd
		L2BlockNumber:  1337,              // nolint:gomnd
		Timestamp:      time.Now().Unix(), // nolint:gomnd
		GlobalExitRoot: [32]byte{10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17},
		Coinbase:       [20]byte{20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24},
		ForkID:         5, // nolint:gomnd
	}
	dataBlockStart := l2blockStart.Encode()

	l2tx := datastreamer.L2Transaction{
		EffectiveGasPricePercentage: 128,                   // nolint:gomnd
		IsValid:                     1,                     // nolint:gomnd
		EncodedLength:               5,                     // nolint:gomnd
		Encoded:                     []byte{1, 2, 3, 4, 5}, // nolint:gomnd
	}
	dataTx := l2tx.Encode()

	l2BlockEnd := datastreamer.L2BlockEnd{
		BlockHash: common.Hash{},
		StateRoot: common.Hash{},
	}
	dataBlockEnd := l2BlockEnd.Encode()

	imark := s.GetHeader().TotalEntries

	// bookmark := []byte("bookmark4800") // Bookmark testing

	end := make(chan uint8)

	go func(chan uint8) {
		var latestRollback uint64 = 0

		rand.Seed(time.Now().UnixNano())

		// Get Bookmark
		// bookEntry, err := s.GetBookmark(bookmark)
		// if err != nil {
		// 	log.Errorf(">> GetBookmark test: error %v", err)
		// } else {
		// 	log.Infof(">> GetBookmark test: entry[%d]", bookEntry)
		// }

		// eventEntry, err := s.GetFirstEventAfterBookmark(bookmark)
		// if err != nil {
		// 	log.Errorf(">> GetFirstEventAfterBookmark test: error %v", err)
		// } else {
		// 	log.Infof(">> GetFirstEventAfterBookmark test: entry[%d] length[%d]", eventEntry.EntryNum, eventEntry.Length)
		// }

		// Get Header
		header := s.GetHeader()
		log.Infof(">> GetHeader test: nextEntryNumber[%d]", header.TotalEntries)

		// Get Entry
		if header.TotalEntries > 10 { // nolint:gomnd
			entry, err := s.GetEntry(10) // nolint:gomnd
			if err != nil {
				log.Errorf(">> GetEntry test: error %v", err)
			} else {
				log.Infof(">> GetEntry test: num[%d] type[%d] length[%d]", entry.Number, entry.Type, entry.Length)
			}
		}

		// for n := 1; n <= 10000; n++ {
		for {
			// Start atomic operation
			err = s.StartAtomicOp()
			if err != nil {
				log.Error(">> App error! StartAtomicOp")
				return
			}

			// Add stream entries:
			// Bookmark
			bookmark := fmt.Sprintf("bookmark%d", imark)
			_, err := s.AddStreamBookmark([]byte(bookmark))
			if err != nil {
				log.Errorf(">> App error! AddStreamBookmark: %v", err)
			}

			// Block Start
			entryBlockStart, err := s.AddStreamEntry(EtL2BlockStart, dataBlockStart)
			if err != nil {
				log.Errorf(">> App error! AddStreamEntry type %v: %v", EtL2BlockStart, err)
				return
			}
			// Tx
			numTx := 1 //rand.Intn(20) + 1
			for i := 1; i <= numTx; i++ {
				_, err = s.AddStreamEntry(EtL2Tx, dataTx)
				if err != nil {
					log.Errorf(">> App error! AddStreamEntry type %v: %v", EtL2Tx, err)
					return
				}
			}
			// Block Start
			entryBlockEnd, err := s.AddStreamEntry(EtL2BlockEnd, dataBlockEnd)
			if err != nil {
				log.Errorf(">> App error! AddStreamEntry type %v: %v", EtL2BlockEnd, err)
				return
			}

			imark = entryBlockEnd + 1

			if entryBlockStart%10 != 0 || latestRollback == entryBlockStart {
				// Commit atomic operation
				err = s.CommitAtomicOp()
				if err != nil {
					log.Error(">> App error! CommitAtomicOp")
					return
				}
			} else {
				// Rollback atomic operation
				err = s.RollbackAtomicOp()
				if err != nil {
					log.Error(">> App error! RollbackAtomicOp")
				}
				latestRollback = entryBlockStart
			}

			// time.Sleep(5000 * time.Millisecond) // nolint:gomnd
		}
		// end <- 0
	}(end)
	// ------------------------------------------------------------

	// Wait for finished
	// <-end

	// Wait for ctl+c
	log.Info(">> Press Control+C to finish...")
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGTERM)
	<-interruptSignal

	log.Info(">> App end")

	return nil
}

// runClient runs a local datastream client and tests its features
func runClient(*cli.Context) error {
	// Create client
	// c, err := datastreamer.NewClient("127.0.0.1:6900", StSequencer)
	c, err := datastreamer.NewClient("stream.internal.zkevm-test.net:6900", StSequencer)
	if err != nil {
		return err
	}

	// Set data entries definition
	entriesDefinition := map[datastreamer.EntryType]datastreamer.EntityDefinition{
		EtL2BlockStart: {
			Name:       "L2BlockStart",
			StreamType: datastreamer.StreamTypeSequencer,
			Definition: reflect.TypeOf(datastreamer.L2BlockStart{}),
		},
		EtL2Tx: {
			Name:       "L2Transaction",
			StreamType: datastreamer.StreamTypeSequencer,
			Definition: reflect.TypeOf(datastreamer.L2Transaction{}),
		},
		EtL2BlockEnd: {
			Name:       "L2BlockEnd",
			StreamType: datastreamer.StreamTypeSequencer,
			Definition: reflect.TypeOf(datastreamer.L2BlockEnd{}),
		},
	}
	c.SetEntriesDef(entriesDefinition)

	// Start client (connect to the server)
	err = c.Start()
	if err != nil {
		return err
	}

	// Command header: Get status
	err = c.ExecCommand(datastreamer.CmdHeader)
	if err != nil {
		return err
	}

	// Command StartBookmark: Sync and start streaming receive from bookmark
	// c.FromBookmark = []byte("bookmark4800")
	// err = c.ExecCommand(datastreamer.CmdStartBookmark)
	// if err != nil {
	// 	return err
	// }

	// Command start: Sync and start streaming receive from entry number
	if c.Header.TotalEntries > 10 { // nolint:gomnd
		c.FromEntry = c.Header.TotalEntries - 10 // nolint:gomnd
	} else {
		c.FromEntry = 0
	}
	err = c.ExecCommand(datastreamer.CmdStart)
	if err != nil {
		return err
	}

	// Run until Ctl+C
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGTERM)
	<-interruptSignal

	// Command stop: Stop streaming
	err = c.ExecCommand(datastreamer.CmdStop)
	if err != nil {
		return err
	}

	log.Info("Client stopped")
	return nil
}
