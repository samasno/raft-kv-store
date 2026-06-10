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
	raft_precandidate
	raft_candidate
	raft_leader
)

type RaftMessageType uint8

// TODO better enumeration on serialized types
const (
	MESSAGE_APPEND RaftMessageType = iota
	MESSAGE_APPEND_RESPONSE
	MESSAGE_PREVOTE_REQUEST
	MESSAGE_PREVOTE_RESPONSE
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

type Raft struct { // implements raftInternal interface
	id           uint64
	currentState raftState
	time         uint64
	logs         []string
	mtx          *sync.Mutex // use if external dependencies come up
	metadataFile RaftMetadataFile
	logFile      RaftLogFile
	peers        []uint64

	call raftCallFn // use transtion functions to reset op pointers
	tick raftTickFn

	currentTerm uint64
	votedFor    uint64
	votes       uint64

	electionTimeout uint64
	electionElapsed uint64

	lastApplied uint64
	commitIndex uint64

	// leaderState raftLeaderState

	outBound []RaftOutput
	outc     chan []RaftOutput
	pending  []RaftOutput
}

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
		r.transitionPrecandidate()
		return
	}
}

func (r *Raft) callPrecandidate(m RaftMessage) {

}

func (r *Raft) tickPrecandidate() {

}

func (r *Raft) transitionPrecandidate() {
	// reset election timeout
	// change state to precandidate
	// update tick and call functions
	// create prevote messages for all peers
	//
	r.resetElectionTimeout()
	r.currentState = raft_precandidate
}

func (r *Raft) transitionCandidate() {
	r.resetElectionTimeout()
	// change state
	r.currentState = raft_candidate
	r.call = r.callCandidate
	r.tick = r.tickCandidate

	// output := RaftOutput{
	// 	Self:        true,
	// 	Type:        OUTPUT_METADATA,
	// 	VotedFor:    r.id,
	// 	CurrentTerm: r.currentTerm + 1,
	// }

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

type raftCallFn func(m RaftMessage)

type raftTickFn func()
