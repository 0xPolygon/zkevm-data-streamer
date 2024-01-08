package datastreamer

import "github.com/0xPolygonHermez/zkevm-data-streamer/log"

// StreamRelay type to manage a data stream relay
type StreamRelay struct {
	client *StreamClient
	server *StreamServer
}

// NewRelay creates a new data stream relay
func NewRelay(server string, port uint16, version uint8, systemID uint64, streamType StreamType, fileName string, cfg *log.Config) (*StreamRelay, error) {
	var r StreamRelay
	var err error

	// Create client side
	r.client, err = NewClient(server, streamType)
	if err != nil {
		log.Errorf("Error creating relay client side: %v", err)
		return nil, err
	}

	// Create server side
	r.server, err = NewServer(port, version, systemID, streamType, fileName, cfg)
	if err != nil {
		log.Errorf("Error creating relay server side: %v", err)
		return nil, err
	}

	// Set function to process entry
	r.client.setProcessEntryFunc(relayEntry, r.server)

	return &r, nil
}

// Start connects and syncs with master server then opens access to relay clients
func (r *StreamRelay) Start() error {
	// Start client side
	err := r.client.Start()
	if err != nil {
		log.Errorf("Error starting relay client: %v", err)
		return err
	}

	// Get total entries from the master server
	err = r.client.ExecCommand(CmdHeader)
	if err != nil {
		log.Errorf("Error executing header command: %v", err)
		return err
	}
	r.server.initEntry = r.client.Header.TotalEntries

	// Start server side before exec command `CmdStart`
	err = r.server.Start()
	if err != nil {
		log.Errorf("Error starting relay server: %v", err)
		return err
	}

	// Sync with master server from latest received entry
	r.client.FromEntry = r.server.GetHeader().TotalEntries
	log.Infof("TotalEntries: RELAY %d of MASTER %d", r.client.FromEntry, r.server.initEntry)
	err = r.client.ExecCommand(CmdStart)
	if err != nil {
		log.Errorf("Error executing start command: %v", err)
		return err
	}

	return nil
}

// relayEntry relays the entry received as client to the clients connected to the server
func relayEntry(e *FileEntry, c *StreamClient, s *StreamServer) error {
	// Start atomic operation
	err := s.StartAtomicOp()
	if err != nil {
		log.Errorf("Error starting atomic op: %v", err)
		return err
	}

	// Add entry
	if e.Type == EtBookmark {
		_, err = s.AddStreamBookmark(e.Data)
	} else {
		_, err = s.AddStreamEntry(e.Type, e.Data)
	}

	// Check if error adding entry
	if err != nil {
		log.Errorf("Error adding entry: %v", err)

		// Rollback atomic operation
		err2 := s.RollbackAtomicOp()
		if err2 != nil {
			log.Errorf("Error rollbacking atomic op: %v", err2)
		}
		return err
	}

	// Commit atomic operation
	err = s.CommitAtomicOp()
	if err != nil {
		log.Errorf("Error committing atomic op: %v", err)
		return err
	}

	return nil
}
