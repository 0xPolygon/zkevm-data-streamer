package main

import (
	"os"
	"reflect"

	"github.com/0xPolygonHermez/zkevm-data-streamer"
	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/0xPolygonHermez/zkevm-data-streamer/tool/config"
	"github.com/0xPolygonHermez/zkevm-data-streamer/tool/db"
	"github.com/urfave/cli/v2"
)

const appName = "zkevm-data-streamer-tool"

var (
	configFileFlag = cli.StringFlag{
		Name:     config.FlagCfg,
		Aliases:  []string{"c"},
		Usage:    "Configuration `FILE`",
		Required: false,
	}
)

func main() {
	app := cli.NewApp()
	app.Name = appName
	app.Version = zkevm.Version

	flags := []cli.Flag{
		&configFileFlag,
	}

	app.Commands = []*cli.Command{
		{
			Name:    "version",
			Aliases: []string{"v"},
			Usage:   "Application version and build",
			Action:  version,
		},
		{
			Name:    "run",
			Aliases: []string{},
			Usage:   "Run the tool",
			Action:  start,
			Flags:   flags,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}

func version(*cli.Context) error {
	zkevm.PrintVersion(os.Stdout)
	return nil
}

func start(cliCtx *cli.Context) error {
	c, err := config.Load(cliCtx)
	if err != nil {
		return err
	}
	log.Infof("Loaded configuration: %+v", c)

	// Init logger
	log.Init(c.StreamServer.Log)
	log.Info("Starting tool")

	// Create a stream server
	streamServer, err := datastreamer.New(c.StreamServer.Port, db.StreamTypeSequencer, c.StreamServer.Filename, &c.StreamServer.Log)
	if err != nil {
		log.Fatal(err)
	}

	// Set entities definition
	entriesDefinition := map[datastreamer.EntryType]datastreamer.EntityDefinition{
		db.EntryTypeL2BlockStart: {
			Name:       "L2BlockStart",
			StreamType: db.StreamTypeSequencer,
			Definition: reflect.TypeOf(db.L2BlockStart{}),
		},
		db.EntryTypeL2Tx: {
			Name:       "L2Transaction",
			StreamType: db.StreamTypeSequencer,
			Definition: reflect.TypeOf(db.L2Transaction{}),
		},
		db.EntryTypeL2BlockEnd: {
			Name:       "L2BlockEnd",
			StreamType: db.StreamTypeSequencer,
			Definition: reflect.TypeOf(db.L2BlockEnd{}),
		},
	}

	streamServer.SetEntriesDef(entriesDefinition)
	err = streamServer.Start()
	if err != nil {
		log.Fatal(err)
	}

	// Connect to the database
	stateSqlDB, err := db.NewSQLDB(c.StateDB)
	if err != nil {
		log.Fatal(err)
	}
	defer stateSqlDB.Close()
	stateDB := db.NewStateDB(stateSqlDB)
	log.Info("Connected to the database")

	var l2blocks []*db.L2Block

	// Get Genesis block
	l2blocks, err = stateDB.GetL2Blocks(cliCtx.Context, 1, 0)
	if err != nil {
		log.Fatal(err)
	}

	if len(l2blocks) == 0 {
		log.Fatal("No genesis block found")
	}

	err = streamServer.StartAtomicOp()
	if err != nil {
		log.Fatal(err)
	}

	genesisBlock := db.L2BlockStart{
		BatchNumber:    l2blocks[0].BatchNumber,
		L2BlockNumber:  l2blocks[0].L2BlockNumber,
		Timestamp:      l2blocks[0].Timestamp,
		GlobalExitRoot: l2blocks[0].GlobalExitRoot,
		Coinbase:       l2blocks[0].Coinbase,
		ForkID:         l2blocks[0].ForkID,
	}

	_, err = streamServer.AddStreamEntry(1, genesisBlock.Encode())
	if err != nil {
		log.Fatal(err)
	}

	err = streamServer.CommitAtomicOp()
	if err != nil {
		log.Fatal(err)
	}

	var limit uint = 1000
	var offset uint = 1
	var entry uint64

	for err == nil {
		log.Infof("Current entry number: %d", entry)

		l2blocks, err = stateDB.GetL2Blocks(cliCtx.Context, limit, offset)
		offset += limit
		if len(l2blocks) == 0 {
			break
		}
		// Get transactions for all the retrieved l2 blocks
		l2Transactions, err := stateDB.GetL2Transactions(cliCtx.Context, l2blocks[0].L2BlockNumber, l2blocks[len(l2blocks)-1].L2BlockNumber)
		if err != nil {
			log.Fatal(err)
		}

		err = streamServer.StartAtomicOp()
		if err != nil {
			log.Fatal(err)
		}

		for x, l2block := range l2blocks {
			blockStart := db.L2BlockStart{
				BatchNumber:    l2block.BatchNumber,
				L2BlockNumber:  l2block.L2BlockNumber,
				Timestamp:      l2block.Timestamp,
				GlobalExitRoot: l2block.GlobalExitRoot,
				Coinbase:       l2block.Coinbase,
				ForkID:         l2block.ForkID,
			}

			_, err = streamServer.AddStreamEntry(db.EntryTypeL2BlockStart, blockStart.Encode())
			if err != nil {
				log.Fatal(err)
			}

			entry, err = streamServer.AddStreamEntry(db.EntryTypeL2Tx, l2Transactions[x].Encode())
			if err != nil {
				log.Fatal(err)
			}

			blockEnd := db.L2BlockEnd{
				BlockHash: l2block.BlockHash,
				StateRoot: l2block.StateRoot,
			}

			_, err = streamServer.AddStreamEntry(db.EntryTypeL2BlockEnd, blockEnd.Encode())
			if err != nil {
				log.Fatal(err)
			}
		}
		err = streamServer.CommitAtomicOp()
		if err != nil {
			log.Fatal(err)
		}
	}

	if err != nil {
		log.Fatal(err)
	}

	log.Info("Finished tool")

	return nil
}
