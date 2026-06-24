package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
)

var magic = "raft"
var metadataFileLength = 132
var votedForOffset = 4
var currentTermOffset = 68
var metadataFilename = "metadata.bin"

type RaftMetadataFile interface {
	CurrentTerm() (uint64, error)
	VotedFor() (uint64, error)
}

type MetadataFile struct {
	dir         string
	filep       *os.File
	votedFor    uint64
	currentTerm uint64
}

func OpenMetadataFile(dirname string) (*MetadataFile, error) {
	// validate directory exists
	info, err := os.Stat(dirname)
	if err != nil {
		return nil, fmt.Errorf("Failed to open %s: %s", dirname, err.Error())
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("invalid directory \"%s\" given for metadata file", dirname)
	}

	mpath := path.Join(dirname, metadataFilename)

	exists := true
	info, err = os.Stat(mpath)
	if err != nil {
		if os.IsNotExist(err) {
			exists = false
		} else {
			return nil, err
		}
	}

	if !exists {
		return newMetadataFile(mpath)
	}

	return useExistingMetadataFile(info, mpath)
}

func useExistingMetadataFile(info os.FileInfo, mpath string) (*MetadataFile, error) {
	if int64(metadataFileLength) != info.Size() {
		return nil, fmt.Errorf("Invalid metadata file. Remove from directory")
	}

	filep, err := os.OpenFile(mpath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	metadataFile := &MetadataFile{}
	metadataFile.filep = filep
	valid, err := readMagicNumber(metadataFile.filep)
	if err != nil {
		defer metadataFile.Close()
		return nil, err
	}

	if !valid {
		defer metadataFile.Close()
		return nil, fmt.Errorf("Invalid metadatafile.")
	}

	metadataFile.readCurrentTerm()
	metadataFile.readVotedFor()
	return metadataFile, nil
}

func newMetadataFile(mpath string) (*MetadataFile, error) {
	mfile := &MetadataFile{}
	var err error
	buf := make([]byte, metadataFileLength)
	copy(buf[0:4], []byte(magic))
	if mfile.filep, err = os.OpenFile(mpath, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return nil, err
	}

	if _, err := mfile.filep.Write(buf); err != nil {
		return nil, err
	}

	if err = mfile.UpdateCurrentTerm(0); err != nil {
		return nil, err
	}

	if err = mfile.UpdateVotedFor(0); err != nil {
		return nil, err
	}

	return mfile, nil
}

func (m *MetadataFile) CurrentTerm() (uint64, error) {
	return m.currentTerm, nil
}

func (m *MetadataFile) VotedFor() (uint64, error) {
	return m.votedFor, nil
}

func (m *MetadataFile) UpdateVotedFor(id uint64) error {
	m.votedFor = id
	return m.writeAt(int64(votedForOffset), m.votedFor)
}

func (m *MetadataFile) UpdateCurrentTerm(term uint64) error {
	m.currentTerm = term
	return m.writeAt(int64(currentTermOffset), m.currentTerm)
}

func (m *MetadataFile) writeAt(offset int64, data uint64) error {
	if nil == m {
		return fmt.Errorf("need to init metadatafile")
	}

	if nil == m.filep {
		return fmt.Errorf("need to init file now")
	}

	m.filep.Seek(offset, io.SeekStart)
	binary.Write(m.filep, binary.LittleEndian, data)

	err := m.filep.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (m *MetadataFile) readCurrentTerm() error {
	m.filep.Seek(int64(currentTermOffset), io.SeekStart)
	err := binary.Read(m.filep, binary.LittleEndian, &m.currentTerm)
	if err != nil {
		return err
	}

	return nil
}

func (m *MetadataFile) readVotedFor() error {
	m.filep.Seek(int64(votedForOffset), io.SeekStart)
	err := binary.Read(m.filep, binary.LittleEndian, &m.votedFor)
	if err != nil {
		return err
	}

	return nil
}

func (m *MetadataFile) Close() error {
	if err := m.filep.Close(); err != nil {
		return err
	}

	m.filep = nil

	return nil
}

func readMagicNumber(f *os.File) (bool, error) {
	buf := make([]byte, 4)

	n, err := f.ReadAt(buf, io.SeekStart)
	if err != nil {
		return false, err
	}

	if n != 4 {
		return false, nil
	}

	if string(buf) != magic {
		return false, nil
	}

	return true, nil
}

func (m *MetadataFile) Filename() string {
	return m.filep.Name()
}
