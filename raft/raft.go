package raft

import (
	"sync"
)

/*
1. deterministic Raft state machine
states
update based on message
track commits + logs
produce outgoing messages to other nodes
storage passed as a read only dependency

* tests

2. deterministic wrapper
provides a access to the private Raft state machine
entire interface used by parent application
maintains deterministic behavior

note - persistence and networking will be handled outside Raft library

*/

type raftState uint8

const (
	raft_follower raftState = iota
	raft_candidate
	raft_leader
)

type RaftMessageType uint8

// TODO better enumeration on serialized types
const (
	MSG_APPEND RaftMessageType = iota
	MSG_VOTE_REQUEST
)

type raftInternal interface {
	call(RaftMessage) // intake messages, update state
	tick()            // advance clock, checks for election
	ready() Ready     // reap outgoing messages into outbound struct, move to pending state
	advance()         // reset state, commit any pending changes to struct
}

type raftCallFn func(m RaftMessage)
type raftTickFn func()

type Raft struct { // implements raftInternal interface
	id           uint64
	currentState raftState
	time         uint64
	logs         []string
	mtx          sync.Mutex // lock for state change reaping

	call raftCallFn // use transtion functions to reset op pointers
	tick raftTickFn

	currentTerm uint64
	votedFor    uint64

	electionTimeout   uint64
	electionCountdown uint64

	// logs
	// storage     RaftStorage // read only input
	lastApplied uint64
	commitIndex uint64

	// leaderState raftLeaderState

	outBound Ready
	pending  Ready
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
func (r *Raft) Call(m RaftMessage) {
	r.call(m)
}

func (r *Raft) Tick() {
	r.tick()
}

func (r *Raft) Advance() {
	println("advance follower")
}

func NewRaft(c RaftConfig) *Raft {
	// validate config
	// supply config to instance
	r := &Raft{}

	r.transitionFollower()

	return r
}

func (r *Raft) transitionFollower() {
	r.currentState = raft_follower
	r.call = r.callFollower
	r.tick = r.tickFollower
}

func (r *Raft) callFollower(m RaftMessage) {
	switch m.Type {
	case MSG_APPEND:
		println("follower append msg")
	case MSG_VOTE_REQUEST:
		println("follower go vote request")
	default:
		panic("Raft in invalid state")
	}
}

func (r *Raft) followerAppendEntry(m RaftMessage) {

}

func (r *Raft) tickFollower() {
	// increment clock
	// increment election timeout
	// if election timeout transition to candidate
	println("tick follower")
}

func (r *Raft) transitionCandidate() {
	r.currentState = raft_candidate
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
	LogIndex    uint64
	Term        uint64
	IsConfigLog bool
	Payload     []byte
}

type ApplicationEntry struct {
	Payload []byte
}

type Ready struct {
}

type RaftConfig struct{}
