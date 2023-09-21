package main

import (
	"math/rand"
	"os"
	"reflect"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/0xPolygonHermez/zkevm-data-streamer/tool/db"
	"github.com/urfave/cli/v2"
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

func runServer(*cli.Context) error {
	log.Info(">> App begin")

	// Create stream server
	s, err := datastreamer.New(6900, StSequencer, "streamfile.bin", nil)
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
		return err
	}

	// time.Sleep(5 * time.Second)

	// ------------------------------------------------------------
	// Fake Sequencer data
	l2block := db.L2Block{
		BatchNumber:    101,
		L2BlockNumber:  1337,
		Timestamp:      time.Now().Unix(),
		GlobalExitRoot: [32]byte{10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17},
		Coinbase:       [20]byte{20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24},
	}
	dataBlock := l2block.Encode()

	l2tx := db.L2Transaction{
		BatchNumber:                 101,
		EffectiveGasPricePercentage: 128,
		IsValid:                     1,
		EncodedLength:               5,
		Encoded:                     []byte{1, 2, 3, 4, 5},
	}
	dataTx := l2tx.Encode()

	end := make(chan uint8)

	go func(chan uint8) {
		var latestRollback uint64 = 0

		rand.Seed(time.Now().UnixNano())

		for n := 1; n <= 10000; n++ {
			// Start atomic operation
			err = s.StartAtomicOp()
			if err != nil {
				log.Error(">> App error! StartAtomicOp")
				return
			}

			// Add stream entries:
			// Block
			entryBlock, err := s.AddStreamEntry(1, dataBlock)
			if err != nil {
				log.Errorf(">> App error! AddStreamEntry type 1: %v", err)
				return
			}
			// Tx
			numTx := 1 //rand.Intn(20) + 1
			for i := 1; i <= numTx; i++ {
				_, err = s.AddStreamEntry(2, dataTx)
				if err != nil {
					log.Errorf(">> App error! AddStreamEntry type 2: %v", err)
					return
				}
			}

			if entryBlock%10 != 0 || latestRollback == entryBlock {
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
				latestRollback = entryBlock
			}

			// time.Sleep(200 * time.Millisecond)
		}
		end <- 0
	}(end)
	// ------------------------------------------------------------

	// Wait for finished
	<-end

	// Wait for ctl+c
	// log.Info(">> Press Control+C to finish...")
	// interruptSignal := make(chan os.Signal, 1)
	// signal.Notify(interruptSignal, os.Interrupt, syscall.SIGTERM)
	// <-interruptSignal

	log.Info(">> App end")

	return nil
}

func runClient(*cli.Context) error {
	// Create client
	// c, err := datastreamer.NewClient("127.0.0.1:6900", StSequencer)
	c, err := datastreamer.NewClient("stream.internal.zkevm-test.net:6900", StSequencer)
	if err != nil {
		return err
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
	c.SetEntriesDefinition(entriesDefinition)

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

	// Command start: Sync and start streaming receive
	if c.Header.TotalEntries > 10 {
		c.FromEntry = c.Header.TotalEntries - 10
	} else {
		c.FromEntry = 0
	}
	err = c.ExecCommand(datastreamer.CmdStart)
	if err != nil {
		return err
	}

	return nil
}
