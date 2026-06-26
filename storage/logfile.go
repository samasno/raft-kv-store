package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/samasno/raft-kv/raft"
)

var _ raft.RaftLogFile = (*LogFile)(nil)
var indexFixedSize = 8 + 8 + 8 + 4
var logEntryHeaderSize = 8 + 8 + 4
var logFilename = "log.bin"
var indexFilename = "index.bin"

type LogFile struct {
	dir          string
	indexfilep   *os.File
	entriesfilep *os.File
	tailIndex    LogIndex
}

// index structure: index u64/term u64/offset u64/length 32
// entry structure: index u64/term u64/length u32/payload n

func (l *LogFile) GetEntry(index uint64) (raft.RaftEntry, error) {
	return l.getEntry(index)
}

func (l *LogFile) GetEntries(first uint64, last uint64) ([]raft.RaftEntry, error) {
	return l.getEntries(first, last)
}

func (l *LogFile) LastLogIndex() (uint64, error) {
	return l.tailIndex.Index, nil
}

func (l *LogFile) LastLogTerm() (uint64, error) {
	return l.tailIndex.Term, nil
}

func (l *LogFile) StartOfTerm(termNumber uint64) (uint64, error) {
	return l.startOfTerm(termNumber)
}

func (l *LogFile) AppendEntries(raftEntries []raft.RaftEntry) error {
	return l.appendEntries(raftEntries)
}

func OpenLogFile(dirname string) (*LogFile, error) {
	info, err := os.Stat(dirname)
	if err != nil {
		return nil, fmt.Errorf("Failed to open %s: %s", dirname, err.Error())
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("invalid directory \"%s\" given for log files", dirname)
	}

	logpath := path.Join(dirname, logFilename)
	indexpath := path.Join(dirname, indexFilename)

	indexExists := true
	indexInfo, err := os.Stat(indexpath)
	if err != nil {
		if os.IsNotExist(err) {
			indexExists = false
		} else {
			return nil, err
		}
	}

	logExists := true
	logInfo, err := os.Stat(logpath)
	if err != nil {
		if os.IsNotExist(err) {
			logExists = false
		} else {
			return nil, err
		}
	}

	if (logExists && !indexExists) || (!logExists && indexExists) {
		return nil, fmt.Errorf("Must remove index.bin and/or log.bin from %s", dirname)
	}

	if !logExists && !indexExists {
		return newLogfile(dirname)
	}

	return useExistingLogfile(logInfo, indexInfo, dirname)
}

func newLogfile(dirname string) (*LogFile, error) {
	logfile := &LogFile{dir: dirname}
	var err error

	indexname := path.Join(logfile.dir, indexFilename)
	logfile.indexfilep, err = os.OpenFile(indexname, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	entriesname := path.Join(logfile.dir, logFilename)
	logfile.entriesfilep, err = os.OpenFile(entriesname, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		defer logfile.Close()
		return nil, err
	}

	if err := writeMagicNumber(logfile.entriesfilep); err != nil {
		defer logfile.Close()
		return nil, err
	}

	if err := writeMagicNumber(logfile.indexfilep); err != nil {
		defer logfile.Close()
		return nil, err
	}

	//return
	return logfile, nil
}

func useExistingLogfile(logInfo, indexInfo os.FileInfo, dirname string) (*LogFile, error) {
	var err error
	entriesname := path.Join(dirname, logInfo.Name())
	indexname := path.Join(dirname, indexInfo.Name())

	logfile := &LogFile{dir: dirname}
	if logfile.entriesfilep, err = os.OpenFile(entriesname, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644); err != nil {
		defer logfile.Close()
		return nil, err
	}

	if logfile.indexfilep, err = os.OpenFile(indexname, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644); err != nil {
		defer logfile.Close()
		return nil, err
	}
	// check magic number entries
	if entriesMagic, _ := readMagicNumber(logfile.entriesfilep); !entriesMagic {
		defer logfile.Close()
		return nil, fmt.Errorf("Encounted invalid entries file %s.", entriesname)
	}

	if indexMagic, _ := readMagicNumber(logfile.indexfilep); !indexMagic {
		defer logfile.Close()
		return nil, fmt.Errorf("Encounted invalid index file %s", indexname)
	}

	// check last index - file length minus index size
	err = logfile.updateLatestIndex()
	if err != nil {
		defer logfile.Close()
		return nil, err
	}

	// TODO validate length against index
	if err = logfile.truncateEntriesToLatestIndex(); err != nil {
		defer logfile.Close()
		return nil, err
	}

	return logfile, nil
}

func (l *LogFile) appendEntries(raftEntries []raft.RaftEntry) error {
	indexes := []LogIndex{}
	entries := []LogEntry{}
	for _, e := range raftEntries {
		index, entry := convertRaftEntryToLogs(l.tailIndex, e)
		indexes = append(indexes, index)
		entries = append(entries, entry)
		l.tailIndex = index
	}
	indexOutput := bytes.NewBuffer([]byte{})
	entriesOutput := bytes.NewBuffer([]byte{})

	for i := 0; i < len(indexes); i++ {
		indexOutput.Write(indexes[i].Marshall())
		entriesOutput.Write(entries[i].Marshall())
	}

	_, err := l.entriesfilep.Write(entriesOutput.Bytes())
	if err != nil {
		return err
	}

	err = l.entriesfilep.Sync()
	if err != nil {
		return err
	}

	_, err = l.indexfilep.Write(indexOutput.Bytes())
	if err != nil {
		return err
	}

	err = l.indexfilep.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (l *LogFile) getEntries(first uint64, last uint64) ([]raft.RaftEntry, error) {
	firstIndex, err := l.fetchIndex(first)
	if err != nil {
		return nil, err
	}

	lastIndex, err := l.fetchIndex(last)
	if err != nil {
		return nil, err
	}

	endOffset := lastIndex.Offset + uint64(logEntryHeaderSize) + uint64(lastIndex.PayloadLength)

	totalLength := endOffset - firstIndex.Offset
	buf := make([]byte, totalLength)

	_, err = l.entriesfilep.ReadAt(buf, int64(firstIndex.Offset))
	if err != nil {
		return nil, err
	}

	entries := []raft.RaftEntry{}
	rd := bytes.NewBuffer(buf)

	for {
		logEntry := LogEntry{}
		logEntry, err = logEntry.Unmarshall(rd)
		if err != nil && err != io.EOF {
			return nil, err
		}

		if err == io.EOF {
			break
		}

		entries = append(entries, logEntry.RaftEntry())
	}

	return entries, nil
}

func (l *LogFile) truncateEntriesToLatestIndex() error {
	if 1 > l.tailIndex.Index {
		return nil
	}

	end := l.tailIndex.Offset + uint64(logEntryHeaderSize) + uint64(l.tailIndex.PayloadLength)
	err := l.entriesfilep.Truncate(int64(end))
	if err != nil {
		return err
	}
	return nil
}

func (l *LogFile) getEntry(index uint64) (raft.RaftEntry, error) {
	le := LogEntry{}
	logIndex, err := l.fetchIndex(index)
	if err != nil {
		return le.RaftEntry(), err
	}

	entryLen := logEntryHeaderSize + int(logIndex.PayloadLength)
	buf := make([]byte, entryLen)
	_, err = l.entriesfilep.ReadAt(buf, int64(logIndex.Offset))
	if err != nil {
		return le.RaftEntry(), err
	}

	le, err = le.Unmarshall(bytes.NewBuffer(buf))
	if err != nil {
		return le.RaftEntry(), err
	}

	return le.RaftEntry(), nil
}

func (l *LogFile) startOfTerm(term uint64) (uint64, error) {
	lo := uint64(1)
	hi := l.tailIndex.Index

	for {
		mid := lo + (hi-lo)/2
		midIndex, err := l.fetchIndex(mid)
		if err != nil {
			return 0, err
		}

		if midIndex.Term < term {
			lo = mid + 1
		} else {
			hi = mid
		}

		if lo == hi {
			break
		}
	}

	return lo, nil
}

func (l *LogFile) updateLatestIndex() (err error) {
	info, err := l.indexfilep.Stat()
	if err != nil {
		return nil
	}

	size := info.Size()
	if size < int64(indexFixedSize) {
		return nil
	}

	lastIndexOffset := size - int64(indexFixedSize)
	buf := make([]byte, indexFixedSize)
	_, err = l.indexfilep.ReadAt(buf, lastIndexOffset)
	if err != nil {
		return err
	}

	l.tailIndex, err = parseLogIndex(bytes.NewBuffer(buf))
	if err != nil {
		return err
	}

	return nil
}

func (l *LogFile) Filenames() (string, string) {
	return l.indexfilep.Name(), l.entriesfilep.Name()
}

func (l *LogFile) Close() error {
	if nil != l.indexfilep {
		err := l.indexfilep.Close()
		if err != nil {
			return err
		}

		l.indexfilep = nil
	}

	if nil != l.entriesfilep {
		err := l.entriesfilep.Close()
		if err != nil {
			return err
		}

		l.entriesfilep = nil
	}

	return nil
}

func parseLogIndex(data io.Reader) (li LogIndex, err error) {
	if err := binary.Read(data, binary.LittleEndian, &li.Index); err != nil {
		return li, err
	}

	if err := binary.Read(data, binary.LittleEndian, &li.Term); err != nil {
		return li, err
	}

	if err := binary.Read(data, binary.LittleEndian, &li.Offset); err != nil {
		return li, err
	}

	if err := binary.Read(data, binary.LittleEndian, &li.PayloadLength); err != nil {
		return li, err
	}

	return li, nil
}

func convertRaftEntryToLogs(last LogIndex, e raft.RaftEntry) (LogIndex, LogEntry) {

	offset := last.Offset + uint64(logEntryHeaderSize) + uint64(last.PayloadLength)

	if 0 == last.Offset {
		offset = 4 // start first entry at 4
	}

	li := LogIndex{
		Offset:        offset,
		Index:         e.Index,
		Term:          e.Term,
		PayloadLength: uint32(len(e.Payload)),
	}

	le := LogEntry{
		Index:         e.Index,
		Term:          e.Term,
		PayloadLength: li.PayloadLength,
		Payload:       e.Payload,
	}

	return li, le
}

func seekIndexPosition(index uint64) (int64, error) {
	if index <= 0 {
		return 0, fmt.Errorf("%d invalid. Must be >= 1", index)
	}

	i := int64(index)
	pos := (i - 1) * int64(indexFixedSize)
	pos += int64(len(magic))
	return pos, nil
}

func (l *LogFile) fetchIndex(index uint64) (LogIndex, error) {
	logIndex := LogIndex{}
	start, err := seekIndexPosition(index)
	if err != nil {
		return logIndex, err
	}

	buf := make([]byte, indexFixedSize)
	_, err = l.indexfilep.ReadAt(buf, start)
	if err != nil {
		return logIndex, err
	}

	logIndex, err = logIndex.Unmarshall(bytes.NewBuffer(buf))
	if err != nil {
		return logIndex, err
	}

	return logIndex, nil
}

func writeMagicNumber(w io.Writer) error {
	buf := []byte(magic)
	if _, err := w.Write(buf); err != nil {
		return err
	}

	return nil
}

func (l *LogFile) debugWalkIndex() {
	l.indexfilep.Seek(4, io.SeekStart)
	println("***Index Walk***")
	for {
		li := LogIndex{}
		li, err := li.Unmarshall(l.indexfilep)
		if err != nil {
			break
		}
		fmt.Printf("i:%d t:%d o:%d l:%d\n", li.Index, li.Term, li.Offset, li.PayloadLength)
	}

	println("***End Index Walk***")
}

func (l *LogFile) debugWalkEntries() {
	l.entriesfilep.Seek(4, io.SeekStart)
	println("***Entries Walk***")
	for {
		le := LogEntry{}
		le, err := le.Unmarshall(l.entriesfilep)
		if err != nil {
			break
		}
		fmt.Printf("i:%d t:%d l:%d p:\"%s\"\n", le.Index, le.Term, le.PayloadLength, string(le.Payload))
	}
	println("***End Entries Walk***")
}
