package main

import (
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/urfave/cli/v2"
)

const (
	StSequencer = 1 // StSequencer sequencer stream type
)

func main() {
	// Set default log level
	log.Init(log.Config{
		Environment: "development",
		Level:       "info",
		Outputs:     []string{"stdout"},
	})

	// Define app
	app := cli.NewApp()
	app.Usage = "Run a datastream relay"
	app.Flags = []cli.Flag{
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
	}
	app.Action = run

	// Run app
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}

// run runs a datastream relay
func run(ctx *cli.Context) error {
	// Set log level
	logLevel := ctx.String("log")
	log.Init(log.Config{
		Environment: "development",
		Level:       logLevel,
		Outputs:     []string{"stdout"},
	})

	log.Info(">> Relay server: begin")

	// CLI parameters
	server := ctx.String("server")
	port := ctx.Uint64("port")
	file := ctx.String("file")
	if server == "" || file == "" || port <= 0 {
		return errors.New("bad/missing parameters")
	}

	// Create relay server
	r, err := datastreamer.NewRelay(server, uint16(port), StSequencer, file, nil)
	if err != nil {
		log.Errorf(">> Relay server: NewRelay error! (%v)", err)
		return err
	}

	// Start relay server
	err = r.Start()
	if err != nil {
		log.Errorf(">> Relay server: Start error! (%v)", err)
		return err
	}

	// Wait for interrupt signal
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGTERM)
	<-interruptSignal

	log.Info(">> Relay server: end")
	return nil
}
