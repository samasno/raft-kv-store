package raft

import "errors"

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
	MESSAGE_APPEND RaftMessageType = iota
	MESSAGE_APPEND_RESPONSE
	MESSAGE_PREVOTE_REQUEST
	MESSAGE_PREVOTE_RESPONSE
	MESSAGE_VOTE_REQUEST
	MESSAGE_VOTE_RESPONSE
)

type RaftMessage struct {
	Type RaftMessageType

	// Common
	To   uint64
	From uint64
	Term uint64

	// Append entry request
	LeaderId         uint64
	PreviousLogIndex uint64
	PreviousLogTerm  uint64
	LeaderCommit     uint64

	Entries []RaftEntry

	// Append entry response
	Success bool

	// Request vote
	CandidateId  uint64
	LastLogIndex uint64
	LastLogTerm  uint64

	// Voting response
	VoteGranted bool
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
	UpdateMetadata  []RaftMetadataUpdate
	SendMessages    []RaftMessage
	WriteLogEntries []RaftEntry
	ApplyEntries    []RaftEntry
}

func (ro *RaftOutput) generateUpdate() *raftUpdate {
	if nil == ro {
		return nil
	}
	update := &raftUpdate{}

	for _, m := range ro.UpdateMetadata {
		update.votedFor = max(m.VotedFor, update.votedFor)
	}

	for _, m := range ro.UpdateMetadata {
		update.currentTerm = max(m.CurrentTerm, update.currentTerm)
	}

	for _, e := range ro.WriteLogEntries {
		update.lastEntryIndex = max(e.Index, update.lastEntryIndex)
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
	lastAppliedIndex uint64
}

type RaftMetadataUpdate struct {
	VotedFor    uint64
	CurrentTerm uint64
}

type RaftConfig struct {
	id uint64
}

func (rc RaftConfig) Validate() error {
	if rc.id == 0 {
		return errors.New("Raft id cannot be 0")
	}

	return nil
}

type RaftLogFile interface {
	LastLogIndex() (uint64, error)
	LastLogTerm() (uint64, error)
}

type RaftMetadataFile interface {
	CurrentTerm() uint64
	VotedFor() uint64
}

type raftCallFn func(m RaftMessage)

type raftTickFn func()
