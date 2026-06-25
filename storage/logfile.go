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
var indexFixedSize = 64 + 64 + 64 + 32
var logEntryHeaderSize = 64 + 64 + 32
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

	// write magic number
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
	if err = logfile.truncateToIndex(); err != nil {
		defer logfile.Close()
		return nil, err
	}

	return logfile, nil
}

func (l *LogFile) AppendEntries(entries []raft.RaftEntry) error {
	// index file made of offset uint64/length uint32 pairs back to back
	// no headers for now, maybe magic number
	// log file term/index/payload. length is in index pair, covers all combined
	// process in batches of 100
	// serialize into index buf and log buf in parallel,
	// once batched or done, write all to file and fsync
	// write to index first
	// write to
	return nil
}

func (l *LogFile) LastLogIndex() (uint64, error) {
	return l.tailIndex.Index, nil
}

func (l *LogFile) LastLogTerm() (uint64, error) {
	return l.tailIndex.Term, nil
}

func (l *LogFile) GetEntries(first uint64, last uint64) ([]raft.RaftEntry, error) {
	// read indexes index/term/offset/length pairs,
	// return error if either index out of bounds
	// calculate bulk read
	// start offset through last offset + last length
	// read and parse into raft entries
	return nil, nil
}

func (l *LogFile) truncateToIndex() error {
	// if index is 0/0 just return
	// check datafile length against last index derived length
	// if shorter, return error
	// if longer, truncate datafile to derived length
	return nil
}

func (l *LogFile) GetEntry(index uint64) (raft.RaftEntry, error) {
	return raft.RaftEntry{}, nil
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
	l.indexfilep.ReadAt(buf, lastIndexOffset)
	l.tailIndex, err = parseLogIndex(bytes.NewBuffer(buf))
	if err != nil {
		return err
	}

	return nil
}

func (l *LogFile) StartOfTerm(termNumber uint64) (uint64, error) {
	// find first index where a term starts, should be a no-op from new leader commit
	// might use a walk back from last index put into in memory map
	// or use a binary search, but this may require more reads
	return 0, nil
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
	offset := last.Offset + uint64(indexFixedSize) + uint64(last.PayloadLength)
	if 0 == last.Offset {
		offset += 4 // account for magic
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

func writeMagicNumber(w io.Writer) error {
	buf := []byte(magic)
	if _, err := w.Write(buf); err != nil {
		return err
	}

	return nil
}
