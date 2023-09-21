package datastreamer

import "github.com/0xPolygonHermez/zkevm-data-streamer/log"

type Config struct {
	// Port to listen on
	Port uint16 `mapstructure:"Port"`
	// Filename of the binary data file
	Filename string `mapstructure:"Filename"`
	// Log
	Log log.Config `mapstructure:"Logs"`
}
