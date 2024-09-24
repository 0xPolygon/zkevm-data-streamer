package datastreamer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProcessCommand(t *testing.T) {
	server := new(StreamServer)
	cli := &client{status: csSyncing}

	// Test CmdStart
	err := server.processCommand(CmdStart, cli)
	assert.EqualError(t, ErrClientAlreadyStarted, err.Error())

	// Test CmdStartBookmark
	err = server.processCommand(CmdStartBookmark, cli)
	assert.EqualError(t, ErrClientAlreadyStarted, err.Error())

	// Test CmdStop
	err = server.processCommand(CmdStop, cli)
	assert.EqualError(t, ErrClientAlreadyStopped, err.Error())

	// Test CmdHeader
	err = server.processCommand(CmdHeader, cli)
	assert.EqualError(t, ErrHeaderCommandNotAllowed, err.Error())

	// Test CmdEntry
	err = server.processCommand(CmdEntry, cli)
	assert.EqualError(t, ErrEntryCommandNotAllowed, err.Error())

	// Test CmdBookmark
	err = server.processCommand(CmdBookmark, cli)
	assert.EqualError(t, ErrBookmarkCommandNotAllowed, err.Error())

	// Test invalid command
	err = server.processCommand(Command(100), cli)
	assert.EqualError(t, ErrInvalidCommand, err.Error())
}
