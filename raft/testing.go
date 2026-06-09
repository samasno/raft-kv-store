package raft

import "errors"

// in memory testing assets for external dependencies
func newInMemoryLogfile(entries []RaftEntry) *inMemoryLogFile {
	if nil == entries {
		entries = []RaftEntry{}
	}

	return &inMemoryLogFile{log: entries}
}

type inMemoryLogFile struct {
	log []RaftEntry
}

func (ms *inMemoryLogFile) LastLogIndex() (uint64, error) {
	if ms.log == nil {
		return 0, errors.New("Logs have not been initialized")
	}

	if len(ms.log) == 0 {
		return 0, nil // a 0 term will indicate no logs gave been appended
	}

	lastLog := ms.log[len(ms.log)-1]

	return lastLog.Index, nil
}

func (ms *inMemoryLogFile) LastLogTerm() (uint64, error) {
	if ms.log == nil {
		return 0, errors.New("Logs have not been initialized")
	}

	if len(ms.log) == 0 {
		return 0, nil // a 0 term will indicate no logs gave been appended
	}

	lastLog := ms.log[len(ms.log)-1]

	return lastLog.Term, nil
}

func appendRaftEntries(ms *inMemoryLogFile, entries []RaftEntry) {
	ms.log = append(ms.log, entries...)
}

type inMemoryMetadataFile struct {
	votedFor    uint64
	currentTerm uint64
}

func newInMemoryMetadataFile(votedFor uint64, currentTerm uint64) *inMemoryMetadataFile {
	return &inMemoryMetadataFile{
		votedFor:    votedFor,
		currentTerm: currentTerm,
	}
}

func (ms *inMemoryMetadataFile) CurrentTerm() uint64 {
	return ms.currentTerm
}

func (ms *inMemoryMetadataFile) VotedFor() uint64 {
	return ms.votedFor
}

func updateInMemoryMetadata(ms *inMemoryMetadataFile, term uint64, voteeId uint64) {
	ms.currentTerm = term
	ms.votedFor = voteeId
}
