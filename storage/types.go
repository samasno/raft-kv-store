package storage

import (
	"bytes"
	"encoding/binary"
	"io"
)

// index structure: index u64/term u64/offset u64/length 32
// entry structure: index u64/term u64/length u32/payload n
type LogEntry struct {
	Index         uint64
	Term          uint64
	PayloadLength uint32
	Payload       []byte
}

func (le LogEntry) Marshall() []byte {
	output := bytes.NewBuffer([]byte{})
	binary.Write(output, binary.LittleEndian, le.Index)
	binary.Write(output, binary.LittleEndian, le.Term)
	binary.Write(output, binary.LittleEndian, le.PayloadLength)
	output.Write(le.Payload)
	return output.Bytes()
}

func (le LogEntry) Unmarshall(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &le.Index); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &le.Term); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &le.PayloadLength); err != nil {
		return err
	}
	le.Payload = make([]byte, le.PayloadLength)
	if _, err := r.Read(le.Payload); err != nil {
		return err
	}

	return nil
}

type LogIndex struct {
	Index         uint64
	Term          uint64
	Offset        uint64
	PayloadLength uint32
}

func (li LogIndex) Marshall() []byte {
	output := bytes.NewBuffer([]byte{})
	binary.Write(output, binary.LittleEndian, li.Index)
	binary.Write(output, binary.LittleEndian, li.Term)
	binary.Write(output, binary.LittleEndian, li.Offset)
	binary.Write(output, binary.LittleEndian, li.PayloadLength)
	return output.Bytes()
}

func (li LogIndex) Unmarshall(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &li.Index); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &li.Term); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &li.Offset); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &li.PayloadLength); err != nil {
		return err
	}

	return nil
}
