package datastreamer

type Config struct {
	// Port to listen on
	Port uint16 `mapstructure:"Port"`
	// Filename of the binary data file
	Filename string `mapstructure:"Filename"`
}
