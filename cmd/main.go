package main

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"strconv"
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
		Level:       "debug",
		Outputs:     []string{"stdout"},
	})

	app := cli.NewApp()
	app.Usage = "Run a datastream server/client/relay demo cli app"

	app.Commands = []*cli.Command{
		{
			Name:    "server",
			Aliases: []string{},
			Usage:   "Run datastream server",
			Flags: []cli.Flag{
				&cli.Uint64Flag{
					Name:        "port",
					Usage:       "exposed port for clients to connect",
					Value:       6900, // nolint:gomnd
					DefaultText: "6900",
				},
				&cli.StringFlag{
					Name:        "file",
					Usage:       "datastream data file name (*.bin)",
					Value:       "datastream.bin",
					DefaultText: "datastream.bin",
				},
				&cli.StringFlag{
					Name:        "log",
					Usage:       "log level (debug|info|warn|error)",
					Value:       "info",
					DefaultText: "info",
				},
				&cli.Uint64Flag{
					Name:        "sleep",
					Usage:       "initial sleep and sleep between atomic operations in ms",
					Value:       0, // nolint:gomnd
					DefaultText: "0",
				},
				&cli.Uint64Flag{
					Name:        "opers",
					Usage:       "number of atomic operations (server will terminate after them)",
					Value:       1000000, // nolint:gomnd
					DefaultText: "1000000",
				},
			},
			Action: runServer,
		},
		{
			Name:    "client",
			Aliases: []string{},
			Usage:   "Run datastream client",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:        "server",
					Usage:       "datastream server address to connect (IP:port)",
					Value:       "127.0.0.1:6900",
					DefaultText: "127.0.0.1:6900",
				},
				&cli.StringFlag{
					Name:        "from",
					Usage:       "entry number to start the sync from (latest|0..N)",
					Value:       "latest",
					DefaultText: "latest",
				},
				&cli.StringFlag{
					Name:  "frombookmark",
					Usage: "bookmark to start the sync from (0..N) (has preference over --from parameter)",
					Value: "none",
				},
				&cli.StringFlag{
					Name:        "log",
					Usage:       "log level (debug|info|warn|error)",
					Value:       "info",
					DefaultText: "info",
				},
			},
			Action: runClient,
		},
		{
			Name:    "relay",
			Aliases: []string{},
			Usage:   "Run datastream relay",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:        "server",
					Usage:       "datastream server address to connect (IP:port)",
					Value:       "127.0.0.1:6900",
					DefaultText: "127.0.0.1:6900",
				},
				&cli.Uint64Flag{
					Name:        "port",
					Usage:       "exposed port for clients to connect",
					Value:       7900, // nolint:gomnd
					DefaultText: "7900",
				},
				&cli.StringFlag{
					Name:        "file",
					Usage:       "relay data file name (*.bin)",
					Value:       "datarelay.bin",
					DefaultText: "datarelay.bin",
				},
				&cli.StringFlag{
					Name:        "log",
					Usage:       "log level (debug|info|warn|error)",
					Value:       "info",
					DefaultText: "info",
				},
			},
			Action: runRelay,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}

// runServer runs a local datastream server and tests its features
func runServer(ctx *cli.Context) error {
	// Set log level
	logLevel := ctx.String("log")
	log.Init(log.Config{
		Environment: "development",
		Level:       logLevel,
		Outputs:     []string{"stdout"},
	})

	log.Info(">> App begin")

	// Parameters
	file := ctx.String("file")
	port := ctx.Uint64("port")
	sleep := ctx.Uint64("sleep")
	numOpersLoop := ctx.Uint64("opers")
	if file == "" || port <= 0 {
		return errors.New("bad/missing parameters")
	}

	// Create stream server
	s, err := datastreamer.NewServer(uint16(port), StSequencer, file, nil)
	if err != nil {
		os.Exit(1)
	}

	// Set data entries definition (optional)
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

	// Pause for testing purpose
	time.Sleep(time.Duration(sleep) * time.Millisecond)

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
	// ------------------------------------------------------------

	imark := s.GetHeader().TotalEntries

	end := make(chan uint8)

	go func(chan uint8) {
		var testRollback bool = false
		var latestRollback uint64 = 0

		rand.Seed(time.Now().UnixNano())

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

		for n := 1; n <= int(numOpersLoop); n++ {
			// Start atomic operation
			err = s.StartAtomicOp()
			if err != nil {
				log.Error(">> App error! StartAtomicOp")
				return
			}

			// Add stream entries:
			// Bookmark
			bookmark := []byte{0} // nolint:gomnd
			bookmark = binary.LittleEndian.AppendUint64(bookmark, imark)

			_, err := s.AddStreamBookmark(bookmark)
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

			if !testRollback || entryBlockStart%10 != 0 || latestRollback == entryBlockStart {
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

			// Pause for testing purpose
			time.Sleep(time.Duration(sleep) * time.Millisecond)
		}
		end <- 0
	}(end)

	// Wait for loop to end
	<-end

	log.Info(">> App end")

	return nil
}

// runClient runs a local datastream client and tests its features
func runClient(ctx *cli.Context) error {
	// Set log level
	logLevel := ctx.String("log")
	log.Init(log.Config{
		Environment: "development",
		Level:       logLevel,
		Outputs:     []string{"stdout"},
	})

	// Parameters
	server := ctx.String("server")
	from := ctx.String("from")
	fromBookmark := ctx.String("frombookmark")
	if server == "" || (from == "" && fromBookmark == "") {
		return errors.New("bad/missing parameters")
	}

	// Create client
	c, err := datastreamer.NewClient(server, StSequencer)
	if err != nil {
		return err
	}

	// Set data entries definition (optional)
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

	// Set process entry callback function
	// c.SetProcessEntryFunc(printEntryNum, nil)

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

	if fromBookmark != "none" {
		// Command StartBookmark: Sync and start streaming receive from bookmark
		fromBookNum, err := strconv.Atoi(fromBookmark)
		if err != nil {
			return err
		}
		bookmark := []byte{0} // nolint:gomnd
		bookmark = binary.LittleEndian.AppendUint64(bookmark, uint64(fromBookNum))
		c.FromBookmark = bookmark
		err = c.ExecCommand(datastreamer.CmdStartBookmark)
		if err != nil {
			return err
		}
	} else {
		// Command start: Sync and start streaming receive from entry number
		if from == "latest" { // nolint:gomnd
			c.FromEntry = c.Header.TotalEntries
		} else {
			fromNum, err := strconv.Atoi(from)
			if err != nil {
				return err
			}
			c.FromEntry = uint64(fromNum)
		}
		err = c.ExecCommand(datastreamer.CmdStart)
		if err != nil {
			return err
		}
	}

	// After the initial sync, run until Ctl+C
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

// printEntryNum prints basic data of the entry
// func printEntryNum(e *datastreamer.FileEntry, c *datastreamer.StreamClient, s *datastreamer.StreamServer) error {
// 	log.Infof("CUSTOM PROCESS: Entry[%d] Type[%d] Length[%d]", e.Number, e.Type, e.Length)
// 	return nil
// }

// runRelay runs a local datastream relay
func runRelay(ctx *cli.Context) error {
	// Set log level
	logLevel := ctx.String("log")
	log.Init(log.Config{
		Environment: "development",
		Level:       logLevel,
		Outputs:     []string{"stdout"},
	})

	log.Info(">> App begin")

	// Parameters
	server := ctx.String("server")
	port := ctx.Uint64("port")
	file := ctx.String("file")
	if server == "" || file == "" || port <= 0 {
		return errors.New("bad/missing parameters")
	}

	// Create relay server
	r, err := datastreamer.NewRelay(server, uint16(port), StSequencer, file, nil)
	if err != nil {
		os.Exit(1)
	}

	// Start relay server
	err = r.Start()
	if err != nil {
		log.Error(">> App error! Start")
		return err
	}

	// Run until Ctl+C
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGTERM)
	<-interruptSignal

	return nil
}
