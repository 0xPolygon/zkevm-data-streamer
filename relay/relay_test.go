package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v2"
)

func TestDefaultConfig(t *testing.T) {
	expectedConfig := &config{
		Server:            "127.0.0.1:6900",
		Port:              7900,
		File:              "datarelay.bin",
		WriteTimeout:      3 * time.Second,
		InactivityTimeout: 120 * time.Second,
		Log:               "info",
	}

	cfg, err := defaultConfig()
	assert.NoError(t, err)
	assert.Equal(t, expectedConfig, cfg)
}

func TestLoadConfig(t *testing.T) {
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_config.toml")
	configContent := `
Server = "127.0.0.1:8080"
Port = 8000
File = "testfile.bin"
WriteTimeout = 2
InactivityTimeout = 60
Log = "debug"
`

	err := os.WriteFile(configFile, []byte(configContent), 0600)
	assert.NoError(t, err)

	app := cli.NewApp()
	set := flag.NewFlagSet("test", 0)
	set.String("cfg", configFile, "doc")
	ctx := cli.NewContext(app, set, nil)

	cfg, err := loadConfig(ctx)
	fmt.Println(cfg)
	assert.NoError(t, err)

	assert.Equal(t, "127.0.0.1:8080", cfg.Server)
	assert.Equal(t, uint64(8000), cfg.Port)
	assert.Equal(t, "testfile.bin", cfg.File)
	assert.Equal(t, 2*time.Nanosecond, cfg.WriteTimeout)
	assert.Equal(t, 60*time.Nanosecond, cfg.InactivityTimeout)
	assert.Equal(t, "debug", cfg.Log)

	viper.Reset()

	os.Setenv("ZKEVM_STREAM_SERVER", "127.0.0.1:9090")
	defer os.Unsetenv("ZKEVM_STREAM_SERVER")

	cfg, err = loadConfig(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1:9090", cfg.Server)
}
