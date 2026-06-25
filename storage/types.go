package storage

// index structure: index u64/term u64/offset u64/length 32
// entry structure: index u64/term u64/length u32/payload n
type LogEntry struct {
	Index         uint64
	Term          uint64
	PayloadLength uint32
	Payload       []byte
}

type LogIndex struct {
	Index         uint64
	Term          uint64
	Offset        uint64
	PayloadLength uint32
}
