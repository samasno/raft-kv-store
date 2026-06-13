package raft

import (
	"errors"
	"fmt"
	"testing"
)

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

func (ms *inMemoryLogFile) GetEntries(startIndex, endIndex uint64) ([]RaftEntry, error) {
	if int(endIndex) > len(ms.log) {
		return nil, fmt.Errorf("Range requested exceeds last entry in log: %d-%d", startIndex, endIndex)
	}

	entries := ms.log[startIndex:endIndex]
	return entries, nil
}

func (ms *inMemoryLogFile) appendRaftEntries(entries []RaftEntry) {
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

func assert(t *testing.T, condition bool, message string) {
	t.Helper()
	if !condition {
		t.Error(message)
	}
}

func assertEqual[T comparable](t *testing.T, name string, actual, expected T) {
	t.Helper()
	if actual != expected {
		t.Errorf("%s: got %v, expected %v", name, actual, expected)
	}
}

func baseValidationCycleOutput(t *testing.T, output *RaftOutput, sendLen, mdataLen, entriesLen, applyLen int) {
	t.Helper()

	assert(t, nil != output, "Output is not nil")
	assertEqual(t, "Messages to send", len(output.SendMessages), sendLen)
	assertEqual(t, "Metadata updates", len(output.UpdateMetadata), mdataLen)
	assertEqual(t, "Entries to write", len(output.WriteLogEntries), entriesLen)
	assertEqual(t, "Entries to apply", len(output.ApplyEntries), applyLen)
}

func setupRaftTest() (*Raft, Raft, *inMemoryMetadataFile, *inMemoryLogFile) {
	id := uint64(9)
	votedFor := uint64(7)
	startTime := uint64(100)

	termOne := generateNEntries(100, 0, 1)
	termTwo := generateNEntries(100, 100, 2)
	termThree := generateNEntries(100, 200, 3)

	mlog := newInMemoryLogfile([]RaftEntry{})
	mlog.appendRaftEntries(termOne)
	mlog.appendRaftEntries(termTwo)
	mlog.appendRaftEntries(termThree)

	err := validateAppendEntriesAreSequential(0, 1, mlog.log)
	if err != nil {
		println(err.Error())
		return nil, Raft{}, nil, nil
	}

	conf := RaftConfig{id}
	mdata := newInMemoryMetadataFile(votedFor, 3)

	r, _ := NewRaftInstance(mdata, mlog, conf)

	r.time = startTime
	r.leader = votedFor
	lastLog, _ := mlog.LastLogIndex()
	r.lastEntryIndex = lastLog
	r.lastAppliedIndex = lastLog
	r.commitIndex = lastLog
	r.currentTerm, _ = mlog.LastLogTerm()

	defaults := *r

	return r, defaults, mdata, mlog
}

func baselineAppendEntryTestMessage(r *Raft, mlog *inMemoryLogFile) RaftMessage {
	m := RaftMessage{
		Type:         MESSAGE_APPEND,
		To:           r.id,
		From:         r.leader,
		Term:         r.currentTerm,
		LeaderCommit: r.commitIndex,
		LeaderId:     r.leader,
		Entries:      nil,
	}

	m.PreviousLogIndex, _ = mlog.LastLogIndex()
	m.PreviousLogTerm, _ = mlog.LastLogTerm()
	return m
}

func cycleNTicks(r *Raft, n int) {
	for i := 0; i < n; i++ {
		r.Tick()
		<-r.Ready()
		r.Advance()
	}
}

// for appending to a log. starts at prevIndex + 1
func generateNEntries(count, prevIndex, startTerm uint64) []RaftEntry {
	output := []RaftEntry{}

	for i := range count {
		inc := i + 1
		entry := RaftEntry{
			Index:   prevIndex + inc,
			Term:    startTerm,
			Payload: make([]byte, 20),
		}

		output = append(output, entry)
	}

	return output
}
