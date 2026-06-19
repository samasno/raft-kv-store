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

	followTracker followTracker

	currentTerm uint64
	votedFor    uint64
	votes       uint64

	electionTimeout uint64
	electionElapsed uint64

	commitIndex      uint64 // in memory only, get from leader
	lastEntryIndex   uint64 // latest raft log written, update after advance
	lastEntryTerm    uint64
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
	r.transitionFollower()
	r.id = conf.id
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

	r.lastEntryTerm, err = r.logFile.LastLogTerm()
	if err != nil {
		panic("Log file failure")
	}

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
	if nil == r.pending {
		return
	}

	r.currentTerm = max(r.currentTerm, r.pending.currentTerm)
	r.lastAppliedIndex = max(r.lastAppliedIndex, r.pending.lastAppliedIndex)
	r.lastEntryIndex = max(r.lastEntryIndex, r.pending.lastEntryIndex)
	r.lastEntryTerm = max(r.lastEntryTerm, r.pending.lastEntryTerm)
	r.votedFor = max(r.votedFor, r.pending.votedFor)

	if 0 != r.pending.votedFor {
		r.leader = r.votedFor
	}

	if r.pending.votedFor == r.id {
		r.votes++
	}

	r.pending = nil
}

func (r *Raft) Done() {
	r.donec <- struct{}{}
}

// FOLLOWER STATE CALLS

func (r *Raft) transitionFollower() {
	r.currentState = raft_follower
	r.tick = r.tickFollower
	r.call = r.callFollower
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

func (r *Raft) callFollower(m RaftMessage) {
	switch m.Type {
	case MESSAGE_APPEND:
		r.followerAppendEntry(m)
	case MESSAGE_PREVOTE_REQUEST:
		r.followerReplyPrevoteRequest(m)
	case MESSAGE_VOTE_REQUEST:
		r.handleVoteRequest(m)
	default:
		r.addResponseToOutput(MESSAGE_INVALID_REQUEST, false, false, m.From)
	}
}

func (r *Raft) followerAppendEntry(m RaftMessage) {
	if m.Term < r.currentTerm {
		r.addAppendEntryResponse(false, m.From)
		return
	}

	if m.LeaderId == 0 {
		r.addAppendEntryResponse(false, m.From)
		return
	}

	r.electionElapsed = 0
	r.leader = m.LeaderId
	r.commitIndex = max(r.commitIndex, m.LeaderCommit)

	r.applyCommittedEntries()

	if m.Term > r.currentTerm {
		update := RaftMetadataUpdate{CurrentTerm: m.Term}
		r.currentTerm = m.Term
		r.addOutboundMetadataUpdate(update)
	}

	err := r.validateEntriesBeforeAppend(m.PreviousLogIndex, m.PreviousLogTerm, m.Entries)
	if err != nil {
		r.addAppendEntryResponse(false, m.From)
		return
	}

	r.addOutboundWriteEntries(m.Entries...)

	r.addAppendEntryResponse(true, m.From)
}

func (r *Raft) followerReplyPrevoteRequest(m RaftMessage) {
	if r.electionElapsed < r.electionTimeout-5 {
		r.addPrevoteResponseToOutput(false, m.From)
		return
	}

	if 0 == m.CandidateId {
		r.addPrevoteResponseToOutput(false, m.From)
		return
	}

	if r.currentTerm > m.PreviousLogTerm {
		r.addPrevoteResponseToOutput(false, m.From)
		return
	}

	if r.currentTerm < m.PreviousLogTerm {
		r.addPrevoteResponseToOutput(true, m.From)
		return
	}

	if r.lastEntryIndex <= m.PreviousLogIndex {
		r.addPrevoteResponseToOutput(true, m.From)
		return
	}

	r.addPrevoteResponseToOutput(false, m.From)
}

// PRECANDIDATE

func (r *Raft) callPrecandidate(m RaftMessage) {
	switch m.Type {
	case MESSAGE_PREVOTE_RESPONSE:
		r.precandidateReceivePrevoteResponse(m)
	case MESSAGE_APPEND:
		r.stepDownToFollowerIfStale(m)
	case MESSAGE_VOTE_REQUEST:
		r.handleVoteRequest(m)
	default:
		r.addResponseToOutput(MESSAGE_INVALID_REQUEST, false, false, m.From)
	}
}

func (r *Raft) precandidateReceivePrevoteResponse(m RaftMessage) {
	// if a rejected vote, return
	if !m.VoteGranted {
		return
	}

	r.votes++

	if r.votes > uint64(len(r.peers)/2) {
		r.transitionCandidate()
	}
}

func (r *Raft) stepDownToFollowerIfStale(m RaftMessage) {
	if m.Term <= r.currentTerm {
		r.addAppendEntryResponse(false, m.From)
		return
	}

	r.updateFollowTracking()
	r.transitionFollower()
	r.callFollower(m)
}

func (r *Raft) tickPrecandidate() {
	if r.currentState != raft_precandidate {
		panic("Raft: tickPrecandidate called from invalid state")
	}
	r.time++
	r.electionElapsed++
	if r.electionElapsed > r.electionTimeout {
		r.transitionPrecandidate()
		return
	}
}

func (r *Raft) transitionPrecandidate() {
	r.resetElectionTimeout()
	r.currentState = raft_precandidate
	r.call = r.callPrecandidate
	r.tick = r.tickPrecandidate
	r.sendPrecandidateCampaign()
}

func (r *Raft) sendPrecandidateCampaign() {
	messages := r.generateBroadcastMessages(MESSAGE_PREVOTE_REQUEST)
	r.addOutboundMessage(messages...)
}

// CANDIDATE
func (r *Raft) transitionCandidate() {
	r.resetElectionTimeout()
	r.currentState = raft_candidate
	r.call = r.callCandidate
	r.tick = r.tickCandidate

	update := RaftMetadataUpdate{VotedFor: r.id, CurrentTerm: r.currentTerm + 1}

	r.addOutboundMetadataUpdate(update)
	r.sendCandidateCampaign()
}

func (r *Raft) sendCandidateCampaign() {
	messages := r.generateBroadcastMessages(MESSAGE_VOTE_REQUEST)
	r.addOutboundMessage(messages...)
}

func (r *Raft) callCandidate(m RaftMessage) {
	switch m.Type {
	case MESSAGE_APPEND:
		r.stepDownToFollowerIfStale(m)
	case MESSAGE_VOTE_RESPONSE:
		r.candidateReceiveVoteResponses(m)
	default:
		r.addResponseToOutput(MESSAGE_INVALID_REQUEST, false, false, m.From)
	}
}

func (r *Raft) candidateReceiveVoteResponses(m RaftMessage) {
	if !m.VoteGranted {
		return
	}

	if m.Term != r.currentTerm {
		return
	}

	r.votes++
	if len(r.peers)/2 < int(r.votes) {
		r.transitionLeader()
	}
}

func (r *Raft) tickCandidate() {
	if r.currentState != raft_candidate {
		panic("Raft: tickCandidate called from invalid state")
	}

	r.time++
	r.electionElapsed++
	if r.electionElapsed > r.electionTimeout {
		r.transitionCandidate()
		return
	}
}

// LEADER

func (r *Raft) transitionLeader() {
	r.currentState = raft_leader
	r.call = r.callLeader
	r.tick = r.tickLeader
	r.leader = r.id
	r.updateFollowTracking()

	r.leaderWriteNewEntries([][]byte{nil})
}

func (r *Raft) callLeader(m RaftMessage) {
	switch m.Type {
	case MESSAGE_NEW_ENTRY:
		r.leaderWriteNewEntries(m.RawEntries)
	case MESSAGE_APPEND:
		r.stepDownToFollowerIfStale(m)
	case MESSAGE_APPEND_RESPONSE:
		println("got response")
	default:
		r.addResponseToOutput(m.Type, false, false, m.From)
	}
}

func (r *Raft) tickLeader() {
	r.time++
	r.leaderSendHeartbeat()
}

func (r *Raft) leaderWriteNewEntries(rawEntries [][]byte) {
	newEntries := []RaftEntry{}

	entryIndex := r.lastEntryIndex + 1
	for _, rawEntry := range rawEntries {
		entry := newEntry(entryIndex, r.currentTerm, rawEntry)
		newEntries = append(newEntries, entry)
		entryIndex++
	}

	r.addOutboundWriteEntries(newEntries...)
	msg := RaftMessage{
		From:             r.id,
		Type:             MESSAGE_APPEND,
		Term:             r.currentTerm,
		PreviousLogIndex: r.lastEntryIndex,
		PreviousLogTerm:  r.lastEntryTerm,
		LeaderCommit:     r.commitIndex,
		Entries:          newEntries,
	}

	r.sendMessageToAllPeers(msg)
}

func (r *Raft) leaderSendHeartbeat() {
	msg := RaftMessage{
		Type:             MESSAGE_APPEND,
		Term:             r.currentTerm,
		LeaderId:         r.id,
		PreviousLogIndex: r.lastEntryIndex,
		PreviousLogTerm:  r.lastEntryTerm,
		LeaderCommit:     r.commitIndex,
	}

	r.sendMessageToAllPeers(msg)
}

func newEntry(index uint64, term uint64, payload []byte) RaftEntry {
	return RaftEntry{
		Index:   index,
		Term:    term,
		Payload: payload,
	}
}

// HELPERS
func (r *Raft) sendMessageToAllPeers(m RaftMessage) {
	messages := []RaftMessage{}

	for _, id := range r.peers {
		shallowCopy := m
		shallowCopy.To = id
		messages = append(messages, shallowCopy)
	}

	r.addOutboundMessage(messages...)
}

func (r *Raft) generateBroadcastMessages(messageType RaftMessageType) []RaftMessage {
	output := []RaftMessage{}

	for _, to := range r.peers {
		msg := genericRaftMessage(messageType, r.id, to)
		msg.PreviousLogIndex = r.lastEntryIndex
		msg.PreviousLogTerm = r.lastEntryTerm

		switch messageType {
		case MESSAGE_PREVOTE_REQUEST:
			msg.CandidateId = r.id
			msg.Term = r.currentTerm
		case MESSAGE_VOTE_REQUEST:
			msg.CandidateId = r.id
			msg.Term = r.currentTerm + 1
		}

		output = append(output, msg)
	}

	return output
}

func (r *Raft) handleVoteRequest(m RaftMessage) {
	if r.currentTerm >= m.Term || r.lastEntryIndex > m.PreviousLogIndex {
		r.addVoteResponseToOutput(false, m.From)
		return
	}

	r.addOutboundMetadataUpdate(RaftMetadataUpdate{VotedFor: m.CandidateId, CurrentTerm: m.Term})
	r.addVoteResponseToOutput(true, m.From)
	r.transitionFollower()
}

func (r *Raft) applyCommittedEntries() {
	if r.lastAppliedIndex == r.commitIndex {
		return
	}

	startIndex := r.lastAppliedIndex
	endIndex := min(r.commitIndex, r.lastEntryIndex)

	entries, err := r.logFile.GetEntries(startIndex, endIndex)
	if err != nil {
		msg := fmt.Sprintf("Attempted to apply committed entries: %s", err.Error())
		panic(msg)
	}

	err = validateEntriesAreSequential(startIndex, entries[0].Term, entries)
	if err != nil {
		msg := fmt.Sprintf("Entries returned from log file: %s", err.Error())
		panic(msg)
	}

	r.addOutboundApplyEntries(entries)
}

func (r *Raft) validateEntriesBeforeAppend(index, term uint64, entries []RaftEntry) error {
	if index != r.lastEntryIndex {
		return fmt.Errorf("Expected log index %d got %d", r.lastEntryIndex, index)
	}

	if term != r.lastEntryTerm {
		return fmt.Errorf("Expected log term %d got %d", r.lastEntryTerm, term)
	}

	if 0 == len(entries) {
		return nil
	}

	err := validateEntriesAreSequential(index, term, entries)
	if err != nil {
		return err
	}

	return nil
}

func (r *Raft) addAppendEntryResponse(success bool, to uint64) {
	r.addResponseToOutput(MESSAGE_APPEND_RESPONSE, success, false, to)
}

func (r *Raft) addPrevoteResponseToOutput(success bool, to uint64) {
	r.addResponseToOutput(MESSAGE_PREVOTE_RESPONSE, success, success, to)
}

func (r *Raft) addVoteResponseToOutput(success bool, to uint64) {
	r.addResponseToOutput(MESSAGE_VOTE_RESPONSE, success, success, to)
}

func (r *Raft) addResponseToOutput(msgType RaftMessageType, success bool, voteGranted bool, to uint64) {
	msg := genericRaftMessage(msgType, r.id, to)
	msg.Success = success
	msg.VoteGranted = voteGranted
	msg.Term = r.currentTerm
	r.addOutboundMessage(msg)
}

func (r *Raft) resetElectionTimeout() {
	r.votes = 0
	r.electionElapsed = 0
	r.electionTimeout = randomTimeout(10, 20)
}

func (r *Raft) loadOutboundToReady() {
	r.pending = r.outbound.generateUpdate()
	r.readyc <- r.outbound
}

func (r *Raft) addOutboundMessage(m ...RaftMessage) {
	if nil == r.outbound {
		r.resetOutbound()
	}

	if nil == r.outbound.SendMessages {
		r.outbound.SendMessages = []RaftMessage{}
	}

	r.outbound.SendMessages = append(r.outbound.SendMessages, m...)
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

func (r *Raft) addOutboundApplyEntries(entries []RaftEntry) {
	if 0 == len(entries) {
		return
	}
	if nil == r.outbound {
		r.resetOutbound()
	}

	if nil == r.outbound.ApplyEntries {
		r.outbound.ApplyEntries = []RaftEntry{}
	}

	r.outbound.ApplyEntries = append(r.outbound.ApplyEntries, entries...)
}

func (r *Raft) addOutboundWriteEntries(entries ...RaftEntry) {
	if 0 == len(entries) {
		return
	}

	if nil == r.outbound {
		r.resetOutbound()
	}

	r.outbound.WriteLogEntries = append(r.outbound.WriteLogEntries, entries...)
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

func (r *Raft) updateFollowTracking() {
	if r.currentState != raft_leader {
		r.followTracker = nil
		return
	}

	tracker := followTracker{}

	for _, id := range r.peers {
		tracker[id] = followerStatus{}
	}

	r.followTracker = tracker
}
