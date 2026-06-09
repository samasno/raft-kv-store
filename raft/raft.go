package raft

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

type raftState uint8

const (
	raft_follower raftState = iota
	raft_candidate
	raft_leader
)

func raftStateString(s raftState) string {
	switch s {
	case raft_follower:
		return "Follower"
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
	MESSAGE_VOTE_REQUEST
	MESSAGE_VOTE_RESPONSE
	MESSAGE_METADATA
	MESSAGE_ENTRIES
)

type RaftOutputType uint8

const (
	OUTPUT_METADATA RaftOutputType = iota
	OUTPUT_MESSAGE
	OUTPUT_ENTRY
	OUTPUT_COMMIT
)

type raftInternal interface {
	call(RaftMessage)  // intake messages, update state
	tick()             // advance clock, checks for election
	ready() RaftOutput // reap outgoing messages into outbound struct, move to pending state
	advance()          // reset state, commit any pending changes to struct
}

type raftCallFn func(m RaftMessage)
type raftTickFn func()

type Raft struct { // implements raftInternal interface
	id           uint64
	currentState raftState
	time         uint64
	logs         []string
	mtx          *sync.Mutex // use if external dependencies come up
	metadataFile RaftMetadataFile
	logFile      RaftLogFile

	call raftCallFn // use transtion functions to reset op pointers
	tick raftTickFn

	currentTerm uint64
	votedFor    uint64

	electionTimeout uint64
	electionElapsed uint64

	lastApplied uint64
	commitIndex uint64

	// leaderState raftLeaderState

	outBound []RaftOutput
	outc     chan []RaftOutput
	pending  []RaftOutput
}

/*
start with follower state functions
- pass messages with the call method
- increment on ticks, steps, advance

  - tick
    increment appropriate counters, skeleton for checks
    no output
  - handleMessage
    types still pending, probably just need append log for now, can do heart beats and logs
    should append logs, 0 out election timeout
    update log and commit indexes. needs storage for lastlog check
    should should create an output for all messages for responses
  - ready
    reaps all logs (no batching for now) puts into pending state
    should output entries for writing
  - advance
    confirms all pending states, updates last applied
    outputs message if any logs were written
*/

func NewRaftInstance(md RaftMetadataFile, log RaftLogFile, conf RaftConfig) (*Raft, error) {
	if nil == md {
		// log here
		return nil, errors.New("No metadata provided")
	}

	if nil == log {
		// log here
		return nil, errors.New("No log provided")
	}

	r := &Raft{}
	r.id = conf.id
	r.currentState = raft_follower
	r.mtx = &sync.Mutex{}
	r.electionTimeout = randomTimeout(10, 20)

	r.metadataFile = md
	r.logFile = log

	var err error
	r.lastApplied, err = r.logFile.LastLogIndex()
	if err != nil {
		// log here
		panic("Log file failure")
	}

	r.call = r.callFollower
	r.tick = r.tickFollower

	r.votedFor = r.metadataFile.VotedFor()
	r.currentTerm = r.metadataFile.CurrentTerm()

	return r, nil
}
func (r *Raft) Call(m RaftMessage) {
	r.call(m)
}

func (r *Raft) Tick() {
	r.tick()
}

func (r *Raft) Advance() {
	println("advance follower")
}

func (r *Raft) transitionFollower() {
	// [TODO] if leader, close leader state, if candidate close candidate state
	r.currentState = raft_follower
	r.call = r.callFollower
	r.tick = r.tickFollower
}

func (r *Raft) callFollower(m RaftMessage) {
	switch m.Type {
	case MESSAGE_APPEND:
		println("follower append entry")
	case MESSAGE_VOTE_REQUEST:
		println("follower go vote request")
	default:
		println("invalid message for follower state")
	}
}

func (r *Raft) followerAppendEntry(m RaftMessage) {
	// check index and term from last log, use storage
	//
}

func (r *Raft) tickFollower() {
	if r.currentState != raft_follower {
		// log here exact state transition
		panic("Raft: tickFollower called from invalid state")
	}
	r.time++
	r.electionElapsed++
	if r.electionElapsed > r.electionTimeout {
		r.resetElectionTimeout()
		r.transitionCandidate()
		return
	}
}

func (r *Raft) transitionCandidate() {
	r.currentState = raft_candidate
	r.currentTerm++
	r.call = r.callCandidate
	r.tick = r.tickCandidate
}

func (r *Raft) callCandidate(m RaftMessage) {
	println("candidate call")
}

func (r *Raft) tickCandidate() {
	println("candidate tick")
}

func (r *Raft) transitionLeader() {
	r.currentState = raft_leader
	r.call = r.callLeader
	r.tick = r.tickLeader
}

func (r *Raft) callLeader(m RaftMessage) {
	println("leader call")
}

func (r *Raft) tickLeader() {
	println("leader tick")
}

func (r *Raft) String() string {
	out := strings.Builder{}
	out.WriteString("*****Raft Instance*****\n")
	out.WriteString(fmt.Sprintf("ID: %d\n", r.id))
	out.WriteString(fmt.Sprintf("State: %s\n", raftStateString(r.currentState)))
	out.WriteString(fmt.Sprintf("Term: %d\n", r.currentTerm))
	out.WriteString(fmt.Sprintf("Voted For: %d\n", r.votedFor))
	out.WriteString(fmt.Sprintf("Time: %d\n", r.time))
	out.WriteString(fmt.Sprintf("Election Timeout: %d\n", r.electionTimeout))
	out.WriteString(fmt.Sprintf("Election Elapsed: %d\n", r.electionElapsed))
	out.WriteString(fmt.Sprintf("Last Log Index: %d\n", r.lastApplied))
	out.WriteString(fmt.Sprintf("Log Count: %d\n", len(r.logs)))

	return out.String()
}

func (r *Raft) resetElectionTimeout() {
	r.electionElapsed = 0
	r.electionTimeout = randomTimeout(10, 20)
}

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
	Self            bool
	Type            RaftOutputType
	Messages        []RaftMessage
	LogEntries      []RaftEntry
	CommitedEntries []RaftEntry
	VotedFor        uint64
	CurrentTerm     uint64
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
