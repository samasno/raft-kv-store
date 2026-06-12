package raft

import (
	"errors"
	"fmt"
	"sync"
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

	callc    chan RaftMessage
	tickc    chan struct{}
	readyc   chan *RaftOutput
	advancec chan struct{}
	donec    chan struct{}
	outbound *RaftOutput
	pending  *raftUpdate
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
	r.callc = make(chan RaftMessage)
	r.tickc = make(chan struct{})
	r.advancec = make(chan struct{})
	r.donec = make(chan struct{})
	r.readyc = make(chan *RaftOutput)

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

	go r.run()

	return r, nil
}

func (r *Raft) run() {
	for {
		r.outbound = nil

		select {
		case m := <-r.callc:
			r.call(m)
		case <-r.tickc:
			r.tick()
		case <-r.donec:
			// graceful shutdown here
			return
		}

		r.loadOutboundToReady()
		<-r.advancec
		r.advance()
	}
}

func (r *Raft) Call(m RaftMessage) {
	r.callc <- m
}

func (r *Raft) Tick() {
	r.tickc <- struct{}{}
}

func (r *Raft) Ready() <-chan *RaftOutput { // run in parent routine
	return r.readyc
}

func (r *Raft) Advance() {
	r.advancec <- struct{}{}
}

func (r *Raft) advance() {

}

func (r *Raft) Done() {
	r.donec <- struct{}{}
}

func (r *Raft) handleCycle(...any) {
	r.resetOutbound()

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
	if m.Term < r.currentTerm {
		r.addAppendEntryResponse(false, m.From)
		r.tickFollower()
		return
	}

	if m.LeaderId == 0 {
		r.addAppendEntryResponse(false, m.From)
		return
	}

	r.time++
	r.electionElapsed = 0
	r.leader = m.LeaderId
	r.commitIndex = max(r.commitIndex, m.LeaderCommit)

	if m.Term > r.currentTerm {
		update := RaftMetadataUpdate{CurrentTerm: m.Term}
		r.addOutboundMetadataUpdate(update)
	}

	if r.commitIndex > r.lastAppliedIndex {
		// create output to catch up commit index or latest written log
		// must be in sync with leader before creating update
	}

	err := r.validatePreviousLogBeforeAppend(m.PreviousLogIndex, m.PreviousLogTerm)
	if err != nil {
		r.addAppendEntryResponse(false, m.From)
		return
	}

	// validate and process entries
	// for i, e := range m.Entries {
	// 	println(i, e)
	// }

	//generate success response

	r.addAppendEntryResponse(true, m.From)
}

func (r *Raft) validatePreviousLogBeforeAppend(index, term uint64) error {
	lastLogIndex, err := r.logFile.LastLogIndex()
	if err != nil {
		panic("Log file failure retrieving index")
	}

	if index != lastLogIndex {
		return fmt.Errorf("Expected log index %d got %d", lastLogIndex, index)
	}

	lastLogTerm, err := r.logFile.LastLogTerm()
	if err != nil {
		panic("Log file failure retrieving term")
	}

	if term != lastLogTerm {
		return fmt.Errorf("Expected log term %d got %d", lastLogTerm, term)
	}

	return nil
}

func (r *Raft) addAppendEntryResponse(success bool, to uint64) {
	failureMessage := RaftMessage{
		Type:    MESSAGE_APPEND_RESPONSE,
		To:      to,
		From:    r.id,
		Success: success,
		Term:    r.currentTerm,
	}

	r.addOutboundMessage(failureMessage)
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

func (r *Raft) resetElectionTimeout() {
	r.electionElapsed = 0
	r.electionTimeout = randomTimeout(10, 20)
}

func (r *Raft) loadOutboundToReady() {
	r.pending = r.outbound.generateUpdate()
	r.readyc <- r.outbound
}

func (r *Raft) addOutboundMessage(m RaftMessage) {
	if nil == r.outbound {
		r.resetOutbound()
	}

	if nil == r.outbound.SendMessages {
		r.outbound.SendMessages = []RaftMessage{}
	}

	r.outbound.SendMessages = append(r.outbound.SendMessages, m)
}

func (r *Raft) addOutboundMetadataUpdate(m RaftMetadataUpdate) {
	if nil == r.outbound {
		r.resetOutbound()
	}

	if nil == r.outbound.UpdateMetadata {
		r.outbound.UpdateMetadata = []RaftMetadataUpdate{}
	}

	r.outbound.UpdateMetadata = append(r.outbound.UpdateMetadata, m)
}

func (r *Raft) addOutboundApplyEntries(e RaftEntry) {
	if nil == r.outbound {
		r.resetOutbound()
	}

	if nil == r.outbound.ApplyEntries {
		r.outbound.ApplyEntries = []RaftEntry{}
	}

	r.outbound.ApplyEntries = append(r.outbound.ApplyEntries, e)
}

func (r *Raft) addOutboundWriteEntries(e RaftEntry) {
	if nil == r.outbound {
		r.resetOutbound()
	}

	if nil == r.outbound.WriteLogEntries {
		r.outbound.WriteLogEntries = []RaftEntry{}
	}

	r.outbound.WriteLogEntries = append(r.outbound.WriteLogEntries, e)
}

func (r *Raft) resetOutbound() {
	ro := &RaftOutput{
		UpdateMetadata:  []RaftMetadataUpdate{},
		SendMessages:    []RaftMessage{},
		WriteLogEntries: []RaftEntry{},
		ApplyEntries:    []RaftEntry{},
	}

	r.outbound = ro
}
