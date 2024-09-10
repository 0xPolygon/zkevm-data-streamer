package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/urfave/cli/v2"
)

const (
	EtL2BlockStart datastreamer.EntryType = 1 // EtL2BlockStart entry type
	EtL2Tx         datastreamer.EntryType = 2 // EtL2Tx entry type
	EtL2BlockEnd   datastreamer.EntryType = 3 // EtL2BlockEnd entry type
	EtUpdateGER    datastreamer.EntryType = 4 // EtUpdateGER entry type

	StSequencer = 1 // StSequencer sequencer stream type

	BookmarkL2Block byte = 0 // BookmarkL2Block bookmark type
	BookmarkBatch   byte = 1 // BookmarkBatch bookmark type

	streamerSystemID = 137
	streamerVersion  = 1

	noneType        = "none"
	streamServerURL = "127.0.0.1:6900"
	logLevelInfo    = "log level (debug|info|warn|error)"
)

var (
	initSanityEntry    bool   = false
	initSanityBlock    bool   = false
	initSanityBookmark bool   = false
	sanityEntry        uint64 = 0
	sanityBlock        uint64 = 0
	sanityBookmark0    uint64 = 0
	sanityBookmark1    uint64 = 0
	dumpBatchNumber    uint64 = 0
	dumpBatchData      string
	initDumpBatch      bool   = false
	dumpEntryFirst     uint64 = 0
	dumpEntryLast      uint64 = 0
	dumpBlockFirst     uint64 = 0
	dumpBlockLast      uint64 = 0
	dumpTotalTx        uint64 = 0
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
					Value:       6900, //nolint:mnd
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
					Usage:       logLevelInfo,
					Value:       "info",
					DefaultText: "info",
				},
				&cli.Uint64Flag{
					Name:        "sleep",
					Usage:       "initial sleep and sleep between atomic operations in ms",
					Value:       0,
					DefaultText: "0",
				},
				&cli.Uint64Flag{
					Name:        "opers",
					Usage:       "number of atomic operations (server will terminate after them)",
					Value:       1000000, //nolint:mnd
					DefaultText: "1000000",
				},
				&cli.Uint64Flag{
					Name:        "writetimeout",
					Usage:       "timeout for write operations on client connections in ms (0=no timeout)",
					Value:       3000, //nolint:mnd
					DefaultText: "3000",
				},
				&cli.Uint64Flag{
					Name:        "inactivitytimeout",
					Usage:       "timeout to kill an inactive client connection in seconds (0=no timeout)",
					Value:       120, //nolint:mnd
					DefaultText: "120",
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
					Value:       streamServerURL,
					DefaultText: streamServerURL,
				},
				&cli.StringFlag{
					Name:        "from",
					Usage:       "entry number to start the sync/streaming from (latest|0..N)",
					Value:       "latest",
					DefaultText: "latest",
				},
				&cli.StringFlag{
					Name:  "frombookmark",
					Usage: "bookmark to start the sync/streaming from (0..N) (has preference over --from parameter)",
					Value: noneType,
				},
				&cli.BoolFlag{
					Name:  "header",
					Usage: "query file header information",
					Value: false,
				},
				&cli.StringFlag{
					Name:  "entry",
					Usage: "entry number to query data (0..N)",
					Value: noneType,
				},
				&cli.StringFlag{
					Name:  "bookmark",
					Usage: "entry bookmark to query entry data pointed by it (0..N)",
					Value: noneType,
				},
				&cli.IntFlag{
					Name:  "bookmarktype",
					Usage: "bookmark type used for --bookmark and --frombookmark options (0..255)",
					Value: 0,
				},
				&cli.BoolFlag{
					Name:  "sanitycheck",
					Usage: "when receiving streaming check entry, bookmark, and block sequence consistency",
					Value: false,
				},
				&cli.StringFlag{
					Name:  "dumpbatch",
					Usage: "batch number to dump data (0..N)",
					Value: noneType,
				},
				&cli.StringFlag{
					Name:        "log",
					Usage:       logLevelInfo,
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
					Value:       streamServerURL,
					DefaultText: streamServerURL,
				},
				&cli.Uint64Flag{
					Name:        "port",
					Usage:       "exposed port for clients to connect",
					Value:       7900, //nolint:mnd
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
					Usage:       logLevelInfo,
					Value:       "info",
					DefaultText: "info",
				},
				&cli.Uint64Flag{
					Name:        "writetimeout",
					Usage:       "timeout for write operations on client connections in ms (0=no timeout)",
					Value:       3000, //nolint:mnd
					DefaultText: "3000",
				},
				&cli.Uint64Flag{
					Name:        "inactivitytimeout",
					Usage:       "timeout to kill an inactive client connection in seconds (0=no timeout)",
					Value:       120, //nolint:mnd
					DefaultText: "120",
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
	writeTimeout := ctx.Uint64("writetimeout")
	inactivityTimeout := ctx.Uint64("inactivitytimeout")

	if file == "" || port <= 0 {
		return errors.New("bad/missing parameters")
	}

	// Create stream server
	s, err := datastreamer.NewServer(uint16(port), streamerVersion, streamerSystemID, StSequencer, file,
		time.Duration(writeTimeout)*time.Millisecond, time.Duration(inactivityTimeout)*time.Second,
		5*time.Second, nil) //nolint:mnd
	if err != nil {
		return err
	}

	// Start stream server
	err = s.Start()
	if err != nil {
		log.Error(">> App error! Start")
		return err
	}

	// Pause for testing purpose
	time.Sleep(time.Duration(sleep) * time.Millisecond)

	end := make(chan uint8)

	go func(chan uint8) {
		var (
			testRollback   bool
			latestRollback uint64
		)

		init := s.GetHeader().TotalEntries / 5 //nolint:mnd

		// Atomic Operations loop
		for n := uint64(0); n < numOpersLoop; n++ {
			// Start atomic operation
			err = s.StartAtomicOp()
			if err != nil {
				log.Error(">> App error! StartAtomicOp")
				return
			}

			// Add stream entries (sample):
			// 1.Bookmark batch
			_, err := s.AddStreamBookmark(fakeBookmark(BookmarkBatch, init+n))
			if err != nil {
				log.Errorf(">> App error! AddStreamBookmark: %v", err)
			}

			// 2.Bookmark L2 block
			_, err = s.AddStreamBookmark(fakeBookmark(BookmarkL2Block, init+n))
			if err != nil {
				log.Errorf(">> App error! AddStreamBookmark: %v", err)
			}
			// 3.Block Start
			entryBlockStart, err := s.AddStreamEntry(EtL2BlockStart, fakeDataBlockStart(init+n))
			if err != nil {
				log.Errorf(">> App error! AddStreamEntry type %v: %v", EtL2BlockStart, err)
				return
			}
			// 4.Tx
			numTx := 1 // rand.Intn(20) + 1
			for i := 1; i <= numTx; i++ {
				_, err = s.AddStreamEntry(EtL2Tx, fakeDataTx())
				if err != nil {
					log.Errorf(">> App error! AddStreamEntry type %v: %v", EtL2Tx, err)
					return
				}
			}
			// 5.Block End
			_, err = s.AddStreamEntry(EtL2BlockEnd, fakeDataBlockEnd(init+n))
			if err != nil {
				log.Errorf(">> App error! AddStreamEntry type %v: %v", EtL2BlockEnd, err)
				return
			}

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

func fakeBookmark(bookType byte, value uint64) []byte {
	bookmark := []byte{bookType}
	bookmark = binary.BigEndian.AppendUint64(bookmark, value)
	return bookmark
}

func fakeDataBlockStart(blockNum uint64) []byte {
	dataBlockStart := make([]byte, 0)
	dataBlockStart = binary.BigEndian.AppendUint64(dataBlockStart, 101) //nolint:mnd
	dataBlockStart = binary.BigEndian.AppendUint64(dataBlockStart, blockNum)
	dataBlockStart = binary.BigEndian.AppendUint64(dataBlockStart, uint64(time.Now().Unix()))
	dataBlockStart = binary.BigEndian.AppendUint32(dataBlockStart, 10)   //nolint:mnd
	dataBlockStart = binary.BigEndian.AppendUint32(dataBlockStart, 1000) //nolint:mnd
	dataBlockStart = append(dataBlockStart,
		[]byte{
			10, 11, 12, 13, 14, 15, 16, 17,
			10, 11, 12, 13, 14, 15, 16, 17,
			10, 11, 12, 13, 14, 15, 16, 17,
			10, 11, 12, 13, 14, 15, 16, 17,
		}...)
	dataBlockStart = append(dataBlockStart,
		[]byte{10, 11, 12, 13, 14, 15, 16, 17,
			10, 11, 12, 13, 14, 15, 16, 17,
			10, 11, 12, 13, 14, 15, 16, 17,
			10, 11, 12, 13, 14, 15, 16, 17,
		}...)
	dataBlockStart = append(dataBlockStart,
		[]byte{
			20, 21, 22, 23, 24, 20, 21, 22, 23, 24,
			20, 21, 22, 23, 24, 20, 21, 22, 23, 24,
		}...)
	dataBlockStart = binary.BigEndian.AppendUint16(dataBlockStart, 5)   //nolint:mnd
	dataBlockStart = binary.BigEndian.AppendUint32(dataBlockStart, 137) //nolint:mnd
	return dataBlockStart
}

func fakeDataTx() []byte {
	dataTx := make([]byte, 0)
	dataTx = append(dataTx, 128, 1) //nolint:mnd
	dataTx = append(dataTx,
		[]byte{
			10, 11, 12, 13, 14, 15, 16, 17,
			10, 11, 12, 13, 14, 15, 16, 17,
			10, 11, 12, 13, 14, 15, 16, 17,
			10, 11, 12, 13, 14, 15, 16, 17}...)
	dataTx = binary.BigEndian.AppendUint32(dataTx, 5) //nolint:mnd
	dataTx = append(dataTx, []byte{1, 2, 3, 4, 5}...)
	return dataTx
}

func fakeDataBlockEnd(blockNum uint64) []byte {
	dataBlockEnd := make([]byte, 0)
	dataBlockEnd = binary.BigEndian.AppendUint64(dataBlockEnd, blockNum)
	dataBlockEnd = append(dataBlockEnd, []byte{
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		1, 2, 3, 4, 5, 6, 7, 8,
		9, 10, 11, 12, 13, 14, 15, 16,
	}...)
	dataBlockEnd = append(dataBlockEnd, []byte{
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		1, 2, 3, 4, 5, 6, 7, 8,
		9, 10, 11, 12, 13, 14, 15, 16,
	}...)
	return dataBlockEnd
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
	if server == "" {
		return errors.New("bad/missing parameters")
	}
	from := ctx.String("from")
	fromBookmark := ctx.String("frombookmark")
	queryHeader := ctx.Bool("header")
	queryEntry := ctx.String("entry")
	queryBookmark := ctx.String("bookmark")
	sanityCheck := ctx.Bool("sanitycheck")
	bookmarkType := ctx.Int("bookmarktype")
	if bookmarkType < 0 || bookmarkType > 255 {
		return errors.New("bad bookmarktype parameter, must be between 0 and 255")
	}
	bookType := byte(bookmarkType)
	paramDumpBatch := ctx.String("dumpbatch")

	// Create client
	c := datastreamer.NewClient(server, StSequencer)

	// Set process entry callback function
	if !sanityCheck {
		if paramDumpBatch != noneType {
			if from == "latest" {
				from = "0"
			}
			nDumpBatch, err := strconv.Atoi(paramDumpBatch)
			if err != nil {
				return err
			}
			dumpBatchNumber = uint64(nDumpBatch)

			c.SetProcessEntryFunc(doDumpBatchData)
		} else {
			c.SetProcessEntryFunc(printEntryNum)
		}
	} else {
		c.SetProcessEntryFunc(checkEntryBlockSanity)
	}

	// Start client (connect to the server)
	c.Start()

	// Query file header information
	if queryHeader {
		header, err := c.ExecCommandGetHeader()
		if err != nil {
			log.Infof("Error: %v", err)
		} else {
			log.Infof("QUERY HEADER: TotalEntries[%d] TotalLength[%d] Version[%d] SystemID[%d]",
				header.TotalEntries, header.TotalLength, header.Version, header.SystemID)
		}
		return nil
	}

	// Query entry option
	if queryEntry != noneType {
		qEntry, err := strconv.Atoi(queryEntry)
		if err != nil {
			return err
		}
		entry, err := c.ExecCommandGetEntry(uint64(qEntry))
		if err != nil {
			log.Infof("Error: %v", err)
		} else {
			log.Infof("QUERY ENTRY %d: Entry[%d] Length[%d] Type[%d] Data[%v]",
				qEntry, entry.Number, entry.Length, entry.Type, entry.Data)
		}
		return nil
	}

	// Query bookmark option
	if queryBookmark != noneType {
		qBookmark, err := strconv.Atoi(queryBookmark)
		if err != nil {
			return err
		}
		qBook := []byte{bookType}
		entry, err := c.ExecCommandGetBookmark(binary.BigEndian.AppendUint64(qBook, uint64(qBookmark)))
		if err != nil {
			log.Infof("Error: %v", err)
		} else {
			log.Infof("QUERY BOOKMARK (%d)%v: Entry[%d] Length[%d] Type[%d] Data[%v]",
				bookType, qBook, entry.Number, entry.Length, entry.Type, entry.Data)
		}
		return nil
	}

	// Command header: Get status
	header, err := c.ExecCommandGetHeader()
	if err != nil {
		return err
	}

	if fromBookmark != noneType {
		// Command StartBookmark: Sync and start streaming receive from bookmark
		fromBookNum, err := strconv.Atoi(fromBookmark)
		if err != nil {
			return err
		}
		bookmark := []byte{bookType}
		err = c.ExecCommandStartBookmark(binary.BigEndian.AppendUint64(bookmark, uint64(fromBookNum)))
		if err != nil {
			return err
		}
	} else {
		// Command start: Sync and start streaming receive from entry number
		var fromEntry uint64
		if from == "latest" {
			fromEntry = header.TotalEntries
		} else {
			fromNum, err := strconv.Atoi(from)
			if err != nil {
				return err
			}
			fromEntry = uint64(fromNum)
		}
		err = c.ExecCommandStart(fromEntry)
		if err != nil {
			return err
		}
	}

	// After the initial sync, run until Ctl+C
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGTERM)
	<-interruptSignal

	// Command stop: Stop streaming
	err = c.ExecCommandStop()
	if err != nil {
		return err
	}

	log.Info("Client stopped")
	return nil
}

// printEntryNum prints basic data of the entry
func printEntryNum(e *datastreamer.FileEntry, c *datastreamer.StreamClient, s *datastreamer.StreamServer) error {
	log.Infof("PROCESS entry(%s): %d | %d | %d | %d", c.ID, e.Number, e.Length, e.Type, len(e.Data))
	return nil
}

// checkEntryBlockSanity checks entry, bookmark, and block sequence consistency
func checkEntryBlockSanity(
	e *datastreamer.FileEntry,
	c *datastreamer.StreamClient,
	s *datastreamer.StreamServer) error {
	// Sanity check initialization
	if !initSanityEntry {
		initSanityEntry = true
		if c.GetFromStream() > 0 {
			sanityEntry = c.GetFromStream()
		} else {
			sanityEntry = 0
		}
	}

	// Log work in progress
	if e.Number%100000 == 0 {
		log.Infof("Checking entry #%d...", e.Number)
	}

	// Sanity check for entry sequence
	if sanityEntry > 0 {
		if e.Number != sanityEntry {
			if e.Number < sanityEntry {
				log.Warnf("(X) SANITY CHECK failed: REPEATED entries? Received[%d] | Entry expected[%d]", e.Number, sanityEntry)
			} else {
				log.Warnf("(X) SANITY CHECK failed: GAP entries? Received[%d] | Entry expected[%d]", e.Number, sanityEntry)
			}
			return errors.New("sanity check failed for entry sequence")
		}
	} else {
		if e.Number != 0 {
			log.Warnf("(X) SANITY CHECK failed: Entry received[%d] | Entry expected[0]", e.Number)
			return errors.New("sanity check failed for entry sequence")
		}
	}
	sanityEntry++

	// Sanity check for block sequence
	if e.Type == EtL2BlockStart {
		blockNum := binary.BigEndian.Uint64(e.Data[8:16])
		if sanityBlock > 0 {
			if blockNum != sanityBlock {
				if blockNum < sanityBlock {
					log.Infof("(X) SANITY CHECK failed (%d): REPEATED blocks? Received[%d] | Block expected[%d]",
						e.Number, blockNum, sanityBlock)
				} else {
					log.Infof("(X) SANITY CHECK failed (%d): GAP blocks? Received[%d] | Block expected[%d]",
						e.Number, blockNum, sanityBlock)
				}
				sanityBlock = blockNum
			}
		} else {
			if blockNum != 0 {
				if initSanityBlock {
					log.Infof("(X) SANITY CHECK failed (%d): Block received[%d] | Block expected[0]", e.Number, blockNum)
					sanityBlock = 0
				} else {
					log.Infof("SANITY CHECK note (%d): First Block received[%d]", e.Number, blockNum)
					sanityBlock = blockNum
				}
				initSanityBlock = true
			}
		}
		sanityBlock++
	}

	// Sanity check for bookmarks
	if e.Type == datastreamer.EtBookmark {
		bookmarkType := e.Data[0]
		bookmarkNum := binary.BigEndian.Uint64(e.Data[1:9])

		switch bookmarkType {
		case BookmarkL2Block:
			if sanityBookmark0 > 0 {
				if bookmarkNum != sanityBookmark0 {
					if bookmarkNum < sanityBookmark0 {
						log.Infof("(X) SANITY CHECK failed (%d): REPEATED L2block bookmarks? Received[%d] | Bookmark expected[%d]",
							e.Number, bookmarkNum, sanityBookmark0)
					} else {
						log.Infof("(X) SANITY CHECK failed (%d): GAP L2block bookmarks? Received[%d] | Bookmark expected[%d]",
							e.Number, bookmarkNum, sanityBookmark0)
					}
					sanityBookmark0 = bookmarkNum
				}
			} else {
				if bookmarkNum != 0 {
					if initSanityBookmark {
						log.Infof("(X) SANITY CHECK failed (%d): L2block Bookmark received[%d] | Bookmark expected[0]",
							e.Number, bookmarkNum)
						sanityBookmark0 = 0
					} else {
						log.Infof("SANITY CHECK note (%d): First L2block Bookmark received[%d]", e.Number, bookmarkNum)
						sanityBookmark0 = bookmarkNum
					}
					initSanityBookmark = true
				}
			}
			sanityBookmark0++

		case BookmarkBatch:
			if sanityBookmark1 > 0 {
				if bookmarkNum != sanityBookmark1 {
					if bookmarkNum < sanityBookmark1 {
						log.Infof("(X) SANITY CHECK failed (%d): REPEATED Batch bookmarks? Received[%d] | Bookmark expected[%d]",
							e.Number, bookmarkNum, sanityBookmark1)
					} else {
						log.Infof("(X) SANITY CHECK failed (%d): GAP Batch bookmarks? Received[%d] | Bookmark expected[%d]",
							e.Number, bookmarkNum, sanityBookmark1)
					}
					sanityBookmark1 = bookmarkNum
				}
			} else {
				if bookmarkNum != 0 {
					if initSanityBookmark {
						log.Infof("(X) SANITY CHECK failed (%d): Batch Bookmark received[%d] | Bookmark expected[0]",
							e.Number, bookmarkNum)
						sanityBookmark1 = 0
					} else {
						log.Infof("SANITY CHECK note (%d): First Batch Bookmark received[%d]", e.Number, bookmarkNum)
						sanityBookmark1 = bookmarkNum
					}
					initSanityBookmark = true
				}
			}
			sanityBookmark1++
		}
	}

	// Sanity check end condition
	if e.Number+1 >= c.GetTotalEntries() {
		log.Infof("SANITY CHECK finished! From entry [%d] to entry [%d]. Latest L2block[%d], Bookmark0[%d], Bookmark1[%d]",
			c.GetFromStream(), c.GetTotalEntries()-1, sanityBlock-1, sanityBookmark0-1, sanityBookmark1-1)
		return errors.New("sanity check finished")
	}

	return nil
}

// doDumpBatchData performs a batch data dump
func doDumpBatchData(e *datastreamer.FileEntry, c *datastreamer.StreamClient, s *datastreamer.StreamServer) error {
	type BatchDump struct {
		Number     uint64 `json:"batchNumber"`
		EntryFirst uint64 `json:"entryFirst"`
		EntryLast  uint64 `json:"entryLast"`
		BlockFirst uint64 `json:"l2BlockFirst"`
		BlockLast  uint64 `json:"l2BlockLast"`
		TotalTx    uint64 `json:"totalTx"`
		Data       string `json:"batchData"`
	}

	if e.Type != EtL2BlockStart && e.Type != EtL2Tx && e.Type != EtL2BlockEnd {
		return nil
	}

	// L2 block start
	if e.Type == EtL2BlockStart {
		batchNumber := binary.BigEndian.Uint64(e.Data[0:8])
		switch {
		case batchNumber < dumpBatchNumber:
			return nil
		case (batchNumber > dumpBatchNumber) || (e.Number+1 >= c.GetTotalEntries()):
			log.Infof("DUMP BATCH finished! First entry[%d], last entry[%d], first block[%d], last block[%d], total tx[%d]",
				dumpEntryFirst, dumpEntryLast, dumpBlockFirst, dumpBlockLast, dumpTotalTx)

			// Dump to json file
			fileName := fmt.Sprintf("dumpbatch%d.json", dumpBatchNumber)
			file, err := os.Create(fileName)
			if err != nil {
				return errors.New("creating dump file")
			}
			defer file.Close()

			bd := BatchDump{
				Number:     dumpBatchNumber,
				EntryFirst: dumpEntryFirst,
				EntryLast:  dumpEntryLast,
				BlockFirst: dumpBlockFirst,
				BlockLast:  dumpBlockLast,
				TotalTx:    dumpTotalTx,
				Data:       dumpBatchData,
			}

			encoder := json.NewEncoder(file)
			err = encoder.Encode(bd)
			if err != nil {
				return errors.New("writing dump file")
			}

			return errors.New("dump batch finished")

		case batchNumber == dumpBatchNumber:
			initDumpBatch = true

			blockNum := binary.BigEndian.Uint64(e.Data[8:16])
			if dumpBlockFirst == 0 {
				dumpBlockFirst = blockNum
			}
			dumpBlockLast = blockNum
		}
	} else if e.Type == EtL2Tx && initDumpBatch {
		dumpTotalTx++
	}

	// Add data
	if initDumpBatch {
		if dumpEntryFirst == 0 {
			dumpEntryFirst = e.Number
		}
		dumpEntryLast = e.Number

		dumpBatchData += fmt.Sprintf("%02x%08x%08x%016x%x", 2, e.Length, e.Type, e.Number, e.Data) //nolint:mnd
	}
	return nil
}

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
	writeTimeout := ctx.Uint64("writetimeout")
	inactivityTimeout := ctx.Uint64("inactivitytimeout")

	// Create relay server
	r, err := datastreamer.NewRelay(server, uint16(port), streamerVersion, streamerSystemID, StSequencer, file,
		time.Duration(writeTimeout)*time.Millisecond, time.Duration(inactivityTimeout)*time.Second,
		5*time.Second, nil) //nolint:mnd
	if err != nil {
		return err
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

	log.Info(">> App end")
	return nil
}
