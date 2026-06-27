package raft

import (
	"errors"
	"fmt"
)

type raftState uint8

const (
	raft_follower raftState = iota
	raft_precandidate
	raft_candidate
	raft_leader
)

func (s raftState) String() string {
	switch s {
	case raft_follower:
		return "Follower"
	case raft_precandidate:
		return "Precandidate"
	case raft_candidate:
		return "Candidate"
	case raft_leader:
		return "Leader"
	}

	return "Invalid State"
}

type RaftMessageType uint8

// TODO better enumeration on serialized types
const (
	MessageAppend RaftMessageType = iota
	MessageAppendResponse
	MessagePrevoteRequest
	MessagePrevoteResponse
	MessageVoteRequest
	MessageVoteResponse
	MessageNewEntry
	MessageInvalidRequest
)

func (rm RaftMessageType) String() string {
	switch rm {
	case MessageAppend:
		return "MessageAppend"
	case MessageAppendResponse:
		return "MessageAppendResponse"
	case MessagePrevoteRequest:
		return "MessagePrevoteRequest"
	case MessagePrevoteResponse:
		return "MessagePrevoteResponse"
	case MessageVoteRequest:
		return "MessageVoteRequest"
	case MessageVoteResponse:
		return "MessageVoteResponse"

	}

	return "INVALID MESSAGE"
}

type RaftMessage struct {
	Type RaftMessageType `json:"type"`

	// Common
	To   uint64 `json:"to"`
	From uint64 `json:"from"`
	Term uint64 `json:"term"`

	// Append entry request
	LeaderId         uint64 `json:"leaderId"`
	PreviousLogIndex uint64 `json:"previousLogIndex"`
	PreviousLogTerm  uint64 `json:"previousLogTerm"`
	LeaderCommit     uint64 `json:"leaderCommit"`

	Entries    []RaftEntry `json:"entries"`
	RawEntries [][]byte    `json:"rawEntries"`

	// Append entry response
	Success bool `json:"success"`

	// Voting
	CandidateId uint64 `json:"candidateId"`
	VoteGranted bool   `json:"voteGranted"`
}

type RaftOutputType uint8

const (
	OUTPUT_METADATA RaftOutputType = iota
	OUTPUT_MESSAGE
	OUTPUT_ENTRY
	OUTPUT_COMMIT
)

type RaftEntry struct {
	Index       uint64
	Term        uint64
	IsConfigLog bool
	Payload     []byte
}

type ApplicationEntry struct {
	Payload []byte
}

type RaftOutput struct {
	UpdateMetadata    []RaftMetadataUpdate
	SendMessages      []RaftMessage
	WriteLogEntries   []RaftEntry
	ApplyEntries      []RaftEntry
	LogFileError      bool
	MetadataFileError bool
}

func (ro *RaftOutput) generateUpdate() *raftUpdate {
	if nil == ro {
		return nil
	}
	update := &raftUpdate{}

	for _, m := range ro.UpdateMetadata {
		if 0 != m.VotedFor {
			update.votedFor = m.VotedFor
		}
		update.currentTerm = max(m.CurrentTerm, update.currentTerm)
	}

	for _, e := range ro.WriteLogEntries {
		update.lastEntryIndex = max(e.Index, update.lastEntryIndex)
		update.lastEntryTerm = max(e.Term, update.lastEntryTerm)
	}

	for _, e := range ro.ApplyEntries {
		update.lastAppliedIndex = max(e.Index, update.lastAppliedIndex)
	}

	return update
}

type raftUpdate struct {
	currentTerm      uint64
	votedFor         uint64
	lastEntryIndex   uint64
	lastEntryTerm    uint64
	lastAppliedIndex uint64
}

type RaftMetadataUpdate struct {
	VotedFor    uint64
	CurrentTerm uint64
}

type followTracker map[uint64]followerStatus

type followerStatus struct {
	lastEntryTerm  uint64
	lastEntryIndex uint64
	isReconciling  bool
}

type RaftConfig struct {
	Id       uint64
	Peers    []uint64
	LogLevel uint8
}

func (rc RaftConfig) Validate() error {
	if rc.Id == 0 {
		return errors.New("Raft id cannot be 0")
	}

	for _, id := range rc.Peers {
		if rc.Id == id {
			return errors.New("Cannot have own id in peers")
		}
	}

	if rc.LogLevel > uint8(Debug) {
		return fmt.Errorf("Max log level is %d", Debug)
	}

	return nil
}

type RaftLogFile interface {
	LastLogIndex() (uint64, error)
	LastLogTerm() (uint64, error)
	GetEntries(start uint64, end uint64) ([]RaftEntry, error)
	GetEntry(index uint64) (RaftEntry, error)
	StartOfTerm(termNumber uint64) (uint64, error)
}

type RaftMetadataFile interface {
	CurrentTerm() (uint64, error)
	VotedFor() (uint64, error)
}

type raftCallFn func(m RaftMessage)

type raftTickFn func()
