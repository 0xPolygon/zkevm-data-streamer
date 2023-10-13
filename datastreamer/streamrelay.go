package datastreamer

import "github.com/0xPolygonHermez/zkevm-data-streamer/log"

// StreamRelay type to manage a data stream relay
type StreamRelay struct {
	client StreamClient
	server StreamServer
}

// NewRelay creates a new data stream relay
func NewRelay(server string, port uint16, streamType StreamType, fileName string, cfg *log.Config) (StreamRelay, error) {
	var r StreamRelay
	var err error

	// Create client side
	r.client, err = NewClient(server, streamType)
	if err != nil {
		log.Errorf("Error creating relay client side: %v", err)
		return r, err
	}

	// Create server side
	r.server, err = NewServer(port, streamType, fileName, cfg)
	if err != nil {
		log.Errorf("Error creating relay server side: %v", err)
		return r, err
	}

	return r, nil
}

// Start connects and syncs with master server then opens access to relay clients
func (r *StreamRelay) Start() error {
	// Start client side
	err := r.client.Start()
	if err != nil {
		log.Errorf("Error starting relay client: %v", err)
		return err
	}

	// Sync with master server from latest received entry
	r.client.FromEntry = r.server.GetHeader().TotalEntries
	err = r.client.ExecCommand(CmdStart)
	if err != nil {
		log.Errorf("Error executing start command: %v", err)
		return err
	}

	// Start server side
	err = r.server.Start()
	if err != nil {
		log.Errorf("Error starting relay server: %v", err)
		return err
	}

	return nil
}
