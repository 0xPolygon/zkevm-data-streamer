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
	log.Init(c.LogConfig)
	log.Info("Starting tool")

	// Create a stream server
	streamServer, err := datastreamer.New(c.StreamServer.Port, db.StreamTypeSequencer, c.StreamServer.Filename)
	if err != nil {
		log.Fatal(err)
	}

	// Set entities definition
	entriesDefinition := map[datastreamer.EntryType]datastreamer.EntityDefinition{
		db.EntryTypeL2Block: {
			Name:       "L2Block",
			StreamType: db.StreamTypeSequencer,
			Definition: reflect.TypeOf(db.L2Block{}),
		},
		db.EntryTypeL2Tx: {
			Name:       "L2Transaction",
			StreamType: db.StreamTypeSequencer,
			Definition: reflect.TypeOf(db.L2Transaction{}),
		},
	}

	streamServer.SetEntriesDefinition(entriesDefinition)

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

	_, err = streamServer.AddStreamEntry(1, l2blocks[0].Encode())
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
			_, err = streamServer.AddStreamEntry(1, l2block.Encode())
			if err != nil {
				log.Fatal(err)
			}

			if l2Transactions[x].BatchNumber == l2block.BatchNumber {
				entry, err = streamServer.AddStreamEntry(2, l2Transactions[x].Encode())
				if err != nil {
					log.Fatal(err)
				}
			} else {
				log.Fatalf("Mismatch between transaction and l2block batch numbers: %d != %d", l2Transactions[x].BatchNumber, l2block.BatchNumber)
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
