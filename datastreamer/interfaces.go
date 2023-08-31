package datastreamer

type streamInterface interface {
	StartStreamTx() error
	AddStreamEntry(etype uint32, data []uint8) (uint64, error)
	CommitStreamTx() error
}
