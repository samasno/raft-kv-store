package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/samasno/raft-kv-store/raft"
)

type LogReader interface {
	GetEntries(start uint64, end uint64) ([]raft.RaftEntry, error)
}

type KVOps string

const (
	GET KVOps = "GET"
	SET KVOps = "SET"
	DEL KVOps = "DEL"
)

type KVMap struct {
	values      map[string]string
	log         LogReader
	lastApplied uint64
	mtx         *sync.Mutex
	checkpoint  *os.File
}

// will be stored in entries as bytes through propose
type Command struct {
	Op    KVOps  `json:"op,omitempty"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type Response struct {
	Success bool   `json:"success"`
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
}

var _ StateMachine = (*KVMap)(nil)
var _ KVStore = (*KVMap)(nil)

func NewKvMap(checkPointFilePath string, log LogReader) (*KVMap, error) {
	kv := &KVMap{
		values: map[string]string{},
		log:    log,
		mtx:    &sync.Mutex{},
	}

	err := kv.GetCheckpoint(checkPointFilePath)
	if err != nil {
		return nil, err
	}

	return kv, nil
}

func (kv *KVMap) set(key, value string) {
	kv.mtx.Lock()
	defer kv.mtx.Unlock()
	kv.values[key] = value
}

func (kv *KVMap) get(key string) string {
	kv.mtx.Lock()
	defer kv.mtx.Unlock()
	return kv.values[key]
}

func (kv *KVMap) delete(key string) {
	kv.mtx.Lock()
	defer kv.mtx.Unlock()
	delete(kv.values, key)
}

func (kv *KVMap) handle(command Command) Response {
	response := Response{Key: command.Key}
	switch command.Op {
	case GET:
		response.Value = kv.get(command.Key)
		response.Success = true
		return response
	case SET:
		kv.set(command.Key, command.Value)
		response.Success = true
		return response
	case DEL:
		kv.delete(command.Key)
		response.Success = true
		return response
	default:
		return response
	}
}

func (kv *KVMap) apply(entries []raft.RaftEntry) error {
	if nil == entries || 0 == len(entries) {
		return nil
	}

	lastIndex := entries[len(entries)-1].Index

	for _, e := range entries {
		command := Command{}
		if err := json.Unmarshal(e.Payload, &command); err != nil {
			// leader will commit nil at beginning of term and break unmarshall, drop nil payloads
			continue
		}

		kv.handle(command)
	}

	err := kv.UpdateCheckpoint(lastIndex)
	if err != nil {
		return err
	}

	return nil
}

func (kv *KVMap) Apply(entries []raft.RaftEntry) error {
	return kv.apply(entries)
}

func (kv *KVMap) Get(key string) string {
	return kv.get(key)
}

func (kv *KVMap) ApplyRecord(entries []raft.RaftEntry) error {
	return kv.apply(entries)
}

func (kv *KVMap) GetCheckpoint(checkpointPath string) error {
	var err error

	if nil == kv.checkpoint {
		kv.checkpoint, err = os.OpenFile(checkpointPath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}
	}

	stat, err := kv.checkpoint.Stat()
	if err != nil {
		return err
	}

	if 8 != stat.Size() && 0 != stat.Size() {
		return fmt.Errorf("Invalid checkpoint file with size %d", stat.Size())
	}

	if 0 == stat.Size() {
		binary.Write(kv.checkpoint, binary.LittleEndian, 0)
		return nil
	}

	kv.checkpoint.Seek(0, io.SeekStart)

	lastApplied := uint64(0)
	err = binary.Read(kv.checkpoint, binary.LittleEndian, &lastApplied)
	if err != nil {
		return err
	}

	if kv.lastApplied < lastApplied {
		return kv.Replay(lastApplied)
	}

	return nil

}

func (kv *KVMap) UpdateCheckpoint(i uint64) error {
	kv.checkpoint.Seek(0, io.SeekStart)
	err := binary.Write(kv.checkpoint, binary.LittleEndian, i)
	if err != nil {
		return err
	}

	err = kv.checkpoint.Sync()
	if err != nil {
		return err
	}

	kv.lastApplied = i

	return nil
}

func (kv *KVMap) Replay(end uint64) error {
	chunk := uint64(99)
	for i := uint64(1); i <= end; i += (chunk + 1) {
		last := min(end, i+chunk)
		entries, err := kv.log.GetEntries(i, last)
		if err != nil {
			return err
		}

		err = kv.apply(entries)
		if err != nil {
			return err
		}
	}

	return nil
}
