package storage

import "github.com/samasno/raft-kv/raft"

type RaftLogFile interface {
	LastLogIndex() (uint64, error)
	LastLogTerm() (uint64, error)
	GetEntries(start uint64, end uint64) ([]raft.RaftEntry, error)
	GetEntry(index uint64) (raft.RaftEntry, error)
	StartOfTerm(termNumber uint64) (uint64, error)
}
