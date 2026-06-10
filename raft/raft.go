package raft

import (
	"errors"
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
	mtx          *sync.Mutex // use if external dependencies come up
	metadataFile RaftMetadataFile
	logFile      RaftLogFile // external read only source
	peers        []uint64
	leader       uint64

	call raftCallFn // use transtion functions to reset op pointers
	tick raftTickFn

	currentTerm uint64
	votedFor    uint64
	votes       uint64

	electionTimeout uint64
	electionElapsed uint64

	commitIndex      uint64 // in memory only, get from leader
	lastEntryIndex   uint64 // latest raft log written, update after advance
	lastAppliedIndex uint64 // applied to host application up to commit index, update after advance

	// leaderState raftLeaderState

	outbound RaftOutput
	ready    chan RaftOutput
	pending  raftUpdate
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
	r.lastEntryIndex, err = r.logFile.LastLogIndex()
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
	r.outbound = newRaftOutput()
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
		r.followerAppendEntry(m)
	case MESSAGE_PREVOTE_REQUEST:
		println("follower handling prevote")
	case MESSAGE_VOTE_REQUEST:
		println("follower go vote request")
	default:
		println("invalid message for follower state")
	}
}

func (r *Raft) followerAppendEntry(m RaftMessage) {
	/*
		check term
			if term is higher, must generate a metadatafile output to update
			if term is lower, must generate a failure message to sender. no further changes made
			increment time

			if term valid
			reset electionElapsed to 0
			update leader, volatile state, no security check
			check log entries
				// term can never be zero
				// last index and term must match that reported from logfile, if not send a failure message
				// if valid, generate output messages that are reaped by ready()

			if term not valid, increment electionElapsed and check
	*/
	if m.Term < r.currentTerm {
		// reject case
		// generate failure response
		r.tickFollower()
		return
	}

	r.time++
	r.electionElapsed = 0
	r.leader = m.From

	if m.Term > r.currentTerm {
		// generate metadata update here
		// append to output batch, must be written first
	}

	// lastLogIndex, err := r.logFile.LastLogIndex()
	// if err != nil {
	// 	// log here
	// 	panic("Log file failure retrieving index")
	// }

	// lastLogTerm, err := r.logFile.LastLogTerm()
	// if err != nil {
	// 	// log here
	// 	panic("Log file failure retrieving term")
	// }

	// for i, e := range m.Entries {
	// 	println(i, e)
	// 	//
	// }

	//generate success response
	success := RaftMessage{
		To:      r.leader,
		From:    r.id,
		Success: true,
		Term:    r.currentTerm,
	}

	r.addOutboundMessage(success)
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

// func (r *Raft) String() string {
// 	out := strings.Builder{}
// 	out.WriteString("*****Raft Instance*****\n")
// 	out.WriteString(fmt.Sprintf("ID: %d\n", r.id))
// 	out.WriteString(fmt.Sprintf("State: %s\n", raftStateString(r.currentState)))
// 	out.WriteString(fmt.Sprintf("Term: %d\n", r.currentTerm))
// 	out.WriteString(fmt.Sprintf("Voted For: %d\n", r.votedFor))
// 	out.WriteString(fmt.Sprintf("Time: %d\n", r.time))
// 	out.WriteString(fmt.Sprintf("Election Timeout: %d\n", r.electionTimeout))
// 	out.WriteString(fmt.Sprintf("Election Elapsed: %d\n", r.electionElapsed))
// 	out.WriteString(fmt.Sprintf("Last Log Index: %d\n", r.lastEntryIndex))

// 	return out.String()
// }

func (r *Raft) resetElectionTimeout() {
	r.electionElapsed = 0
	r.electionTimeout = randomTimeout(10, 20)
}

func (r *Raft) addOutboundMessage(m RaftMessage) {
	if nil == r.outbound.SendMessages {
		r.outbound.SendMessages = []RaftMessage{}
	}

	r.outbound.SendMessages = append(r.outbound.SendMessages, m)
}

func (r *Raft) addOutboundMetadataUpdate(m RaftMetadataUpdate) {
	if nil == r.outbound.UpdateMetadata {
		r.outbound.UpdateMetadata = []RaftMetadataUpdate{}
	}
	r.outbound.UpdateMetadata = append(r.outbound.UpdateMetadata, m)
}

func (r *Raft) addOutboundApplyEntries(e RaftEntry) {
	if nil == r.outbound.ApplyEntries {
		r.outbound.ApplyEntries = []RaftEntry{}
	}

	r.outbound.ApplyEntries = append(r.outbound.ApplyEntries, e)
}

func (r *Raft) addOutboundWriteEntries(e RaftEntry) {
	if nil == r.outbound.WriteLogEntries {
		r.outbound.WriteLogEntries = []RaftEntry{}
	}

	r.outbound.WriteLogEntries = append(r.outbound.WriteLogEntries, e)
}

func newRaftOutput() RaftOutput {
	ro := RaftOutput{
		UpdateMetadata:  []RaftMetadataUpdate{},
		SendMessages:    []RaftMessage{},
		WriteLogEntries: []RaftEntry{},
		ApplyEntries:    []RaftEntry{},
	}

	return ro
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
	UpdateMetadata  []RaftMetadataUpdate
	SendMessages    []RaftMessage
	WriteLogEntries []RaftEntry
	ApplyEntries    []RaftEntry
}

func (ro RaftOutput) getUpdate() raftUpdate {
	update := raftUpdate{}

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
