package main

import (
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"github.com/urfave/cli/v2"
)

const (
	StSequencer = 1 // StSequencer sequencer stream type

	streamerSystemID = 137
	streamerVersion  = 1
)

type config struct {
	Server            string
	Port              uint64
	File              string
	WriteTimeout      time.Duration
	InactivityTimeout time.Duration
	Log               string
}

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
			Name:     "cfg",
			Aliases:  []string{"c"},
			Usage:    "Configuration file",
			Required: false,
		},
		&cli.StringFlag{
			Name:  "server",
			Usage: "datastream server address to connect (IP:port)",
		},
		&cli.Uint64Flag{
			Name:  "port",
			Usage: "exposed port for clients to connect",
		},
		&cli.StringFlag{
			Name:  "file",
			Usage: "relay data file name (*.bin)",
		},
		&cli.StringFlag{
			Name:  "log",
			Usage: "log level (debug|info|warn|error)",
		},
		&cli.Uint64Flag{
			Name:  "writetimeout",
			Usage: "timeout for write operations on client connections in ms (0=no timeout)",
		},
		&cli.Uint64Flag{
			Name:  "inactivitytimeout",
			Usage: "timeout to kill an inactive client connection in seconds (0=no timeout)",
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

// defaultConfig parses the default configuration values
func defaultConfig() (*config, error) {
	viper.SetConfigType("toml")

	return &config{
		Server:            "127.0.0.1:6900",
		Port:              7900, //nolint:mnd
		File:              "datarelay.bin",
		WriteTimeout:      3 * time.Second,                  //nolint:mnd
		InactivityTimeout: time.Duration(120 * time.Second), //nolint:mnd
		Log:               "info",
	}, nil
}

// loadConfig loads the configuration
func loadConfig(ctx *cli.Context) (*config, error) {
	cfg, err := defaultConfig()
	if err != nil {
		return nil, err
	}

	configFilePath := ctx.String("cfg")
	if configFilePath != "" {
		dirName, fileName := filepath.Split(configFilePath)

		fileExtension := strings.TrimPrefix(filepath.Ext(fileName), ".")
		fileNameWithoutExtension := strings.TrimSuffix(fileName, "."+fileExtension)

		viper.AddConfigPath(dirName)
		viper.SetConfigName(fileNameWithoutExtension)
		viper.SetConfigType(fileExtension)
	}

	viper.AutomaticEnv()
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.SetEnvPrefix("ZKEVM_STREAM")

	err = viper.ReadInConfig()
	if err != nil {
		var configErr *viper.ConfigFileNotFoundError
		if errors.As(err, &configErr) {
			log.Infof("%w", configErr)
		} else {
			log.Infof("error reading config file: %w", err)
			return nil, err
		}
	}

	decodeHooks := []viper.DecoderConfigOption{
		// this allows arrays to be decoded from env var separated by ",", example: MY_VAR="value1,value2,value3"
		viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
			mapstructure.TextUnmarshallerHookFunc(),
			mapstructure.StringToSliceHookFunc(","))),
	}

	err = viper.Unmarshal(&cfg, decodeHooks...)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

// run runs a datastream relay
func run(ctx *cli.Context) error {
	// Load config file
	cfg, err := loadConfig(ctx)
	if err != nil {
		return err
	}

	// Overwrite with CLI parameters
	port := ctx.Uint64("port")
	if port != 0 {
		cfg.Port = port
	}

	server := ctx.String("server")
	if server != "" {
		cfg.Server = server
	}

	file := ctx.String("file")
	if file != "" {
		cfg.File = file
	}

	logLevel := ctx.String("log")
	if logLevel != "" {
		cfg.Log = logLevel
	}

	writeTimeout := ctx.Uint64("writetimeout")
	if writeTimeout != 0 {
		cfg.WriteTimeout = time.Duration(writeTimeout * uint64(time.Second))
	}

	inactivityTimeout := ctx.Uint64("inactivitytimeout")
	if inactivityTimeout != 0 {
		cfg.InactivityTimeout = time.Duration(inactivityTimeout * uint64(time.Second))
	}

	// Set log level
	log.Init(log.Config{
		Environment: "development",
		Level:       cfg.Log,
		Outputs:     []string{"stdout"},
	})

	log.Infof(">> Relay server started: port[%d] file[%s] server[%s] log[%s]", cfg.Port, cfg.File, cfg.Server, cfg.Log)

	// Create relay server
	r, err := datastreamer.NewRelay(cfg.Server, uint16(cfg.Port), streamerVersion, streamerSystemID,
		StSequencer, cfg.File, cfg.WriteTimeout, cfg.InactivityTimeout, 5*time.Second, nil)
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

	log.Info(">> Relay server finished")
	return nil
}
