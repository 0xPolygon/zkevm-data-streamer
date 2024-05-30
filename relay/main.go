package main

import (
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"github.com/urfave/cli/v2"
)

const (
	StSequencer = 1 // StSequencer sequencer stream type
)

type config struct {
	Server     string
	Port       uint64
	File       string
	Log        string
	DeleteData bool
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
	cfg := config{
		Server:     "127.0.0.1:6900",
		Port:       7900, // nolint:gomnd
		File:       "datarelay.bin",
		Log:        "info",
		DeleteData: false,
	}

	viper.SetConfigType("toml")
	return &cfg, nil
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
		_, ok := err.(viper.ConfigFileNotFoundError)
		if ok {
			log.Infof("config file not found")
		} else {
			log.Infof("error reading config file: ", err)
			return nil, err
		}
	}

	decodeHooks := []viper.DecoderConfigOption{
		// this allows arrays to be decoded from env var separated by ",", example: MY_VAR="value1,value2,value3"
		viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(mapstructure.TextUnmarshallerHookFunc(), mapstructure.StringToSliceHookFunc(","))),
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

	// Set log level
	log.Init(log.Config{
		Environment: "development",
		Level:       cfg.Log,
		Outputs:     []string{"stdout"},
	})

	log.Infof(">> Relay server started: port[%d] file[%s] server[%s] log[%s] delete data[%v]", cfg.Port, cfg.File, cfg.Server, cfg.Log, cfg.DeleteData)

	if cfg.DeleteData {
		log.Warnf(">> Warning Deleting data file: %s", cfg.File)
		deleteDataFile(cfg.File)
		log.Infof(">> Data file deleted: %s succeeded!", cfg.File)
		return nil
	}

	// Create relay server
	r, err := datastreamer.NewRelay(cfg.Server, uint16(cfg.Port), 1, 137, StSequencer, cfg.File, nil) // nolint:gomnd
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

func deleteDataFile(fileName string) {
	ind := strings.IndexRune(fileName, '.')
	if ind == -1 {
		fileName = fileName + ".bin"
	}

	_, err := os.Stat(fileName)
	if os.IsExist(err) {
		err := os.Remove(fileName)
		if err != nil {
			log.Error("Error deleting file:", err)
		} else {
			log.Infof("File deleted: %s", fileName)
		}
	} else {
		log.Infof("File not found: %s", fileName)
	}

	name := fileName[0:strings.IndexRune(fileName, '.')] + ".db"

	_, err = os.Stat(name)
	if os.IsExist(err) {
		err = os.RemoveAll(name)
		if err != nil {
			log.Errorf("Error deleting folder:", err)
		} else {
			log.Infof("Folder deleted: %s", name)
		}
	} else {
		log.Infof("Folder not found: %s", name)
	}
}
