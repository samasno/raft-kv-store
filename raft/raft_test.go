package raft

import (
	"testing"
)

func TestFollowerTicks(t *testing.T) {
	r, defaults, _, _ := setupRaftTest()
	r.electionTimeout = 4

	for i := uint64(1); i <= 4; i++ {
		r.Tick()
		<-r.Ready()
		expectedTime := defaults.time + i
		assertEqual(t, "Time", r.time, expectedTime)
		assertEqual(t, "State", r.currentState.String(), raft_follower.String())
		assertEqual(t, "Election Elapsed", r.electionElapsed, i)
		r.Advance()
	}

	r.Tick()
	<-r.Ready()
	r.Advance()
	r.Done()

	assertEqual(t, "State", r.currentState.String(), raft_precandidate.String())
	assertEqual(t, "Election Elapsed", r.electionElapsed, 0)
	assertEqual(t, "Voted For", r.votedFor, defaults.votedFor)
	assertEqual(t, "Current Term", r.currentTerm, defaults.currentTerm)
	assertEqual(t, "Last Log Index", r.lastEntryIndex, defaults.lastEntryIndex)
	assert(t, r.electionTimeout > 9, "Election Timeout: Expected to be greater than 9")
}

func TestCallFollowerHeartbeatSameTerm(t *testing.T) {
	r, defaults, _, mlog := setupRaftTest()
	for i := range uint64(4) {
		r.Tick()
		<-r.Ready()
		r.Advance()
		assertEqual(t, "Election elapsed increments", r.electionElapsed, defaults.electionElapsed+uint64(1)+i)
		assertEqual(t, "Raft should not change", r.currentState.String(), defaults.currentState.String())
	}

	heartbeat := RaftMessage{
		Type:         MESSAGE_APPEND,
		To:           r.id,
		From:         r.leader,
		Term:         r.currentTerm,
		LeaderCommit: r.commitIndex,
		LeaderId:     r.leader,
		Entries:      nil,
	}

	heartbeat.PreviousLogIndex, _ = mlog.LastLogIndex()
	heartbeat.PreviousLogTerm, _ = mlog.LastLogTerm()

	r.Call(heartbeat)
	output := <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 1, 0, 0, 0)
	assertEqual(t, "Election elapsed goes back to 0", r.electionElapsed, 0)

	successMessage := output.SendMessages[0]
	assertEqual(t, "Success response", successMessage.Success, true)
	assertEqual(t, "Got RESPONSE type message", successMessage.Type.String(), MESSAGE_APPEND_RESPONSE.String())
	assertEqual(t, "Correct TO in response", successMessage.To, r.leader)
	assertEqual(t, "Correct FROM in response", successMessage.From, r.id)
	assertEqual(t, "Expected current term", successMessage.Term, r.currentTerm)
}

func TestCallFollowerHeartbeatOldTermLeader(t *testing.T) {
	r, _, _, mlog := setupRaftTest()

	n := 5
	cycleNTicks(r, n)

	assertEqual(t, "Expected elapsedElection to increase", r.electionElapsed, uint64(n))

	heartbeat := RaftMessage{
		Type:         MESSAGE_APPEND,
		To:           r.id,
		From:         r.leader,
		Term:         r.currentTerm - 1,
		LeaderCommit: r.commitIndex,
		LeaderId:     r.leader,
		Entries:      nil,
	}

	heartbeat.PreviousLogIndex, _ = mlog.LastLogIndex()
	heartbeat.PreviousLogTerm, _ = mlog.LastLogTerm()

	r.Call(heartbeat)
	output := <-r.Ready()
	r.Advance()

	assertEqual(t, "Election elapsed to increase", r.electionElapsed, uint64(n))
	baseValidationCycleOutput(t, output, 1, 0, 0, 0)

	failure := output.SendMessages[0]
	assertEqual(t, "Expect failure response", failure.Success, false)
	assertEqual(t, "Correct TO in response", failure.To, r.leader)
	assertEqual(t, "Got RESPONSE type message", failure.Type.String(), MESSAGE_APPEND_RESPONSE.String())
	assertEqual(t, "Correct FROM in response", failure.From, r.id)
	assertEqual(t, "Expected current term", failure.Term, r.currentTerm)
}

func TestCallFollowerHeartbeatLaterTermLeader(t *testing.T) {
	r, _, _, _ := setupRaftTest()
	msg := baselineAppendEntryTestMessage(r)
	newterm := uint64(1239349)
	msg.Term = newterm

	r.Call(msg)
	output := <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 1, 1, 0, 0)

	metadataUpdate := output.UpdateMetadata[0]
	assertEqual(t, "Metadata updates for term", metadataUpdate.CurrentTerm, newterm)
	assertEqual(t, "Pending update must be nil after advance", r.pending, nil)
	assertEqual(t, "Raft term updated", r.currentTerm, newterm)

}

func TestCallFollowerAppendEntriesWrongIndex(t *testing.T) {
	r, _, _, _ := setupRaftTest()

	msga := baselineAppendEntryTestMessage(r)

	r.Call(msga)
	output := <-r.Ready()
	r.Advance()

	baseline := output.SendMessages[0]
	assertEqual(t, "Baseline success", baseline.Success, true)

	msgb := baselineAppendEntryTestMessage(r)
	msgb.PreviousLogIndex = 111111

	r.Call(msgb)
	output = <-r.Ready()
	r.Advance()

	indexFail := output.SendMessages[0]
	assertEqual(t, "Incorrect index fails", indexFail.Success, false)

	msgc := baselineAppendEntryTestMessage(r)
	msgc.PreviousLogTerm = 111111

	r.Call(msgc)
	output = <-r.Ready()
	r.Advance()

	termFail := output.SendMessages[0]
	assertEqual(t, "Incorrect term fails", termFail.Success, false)
}

func TestCallFollowerAppendEntriesWriteEntries(t *testing.T) {
	r, defaults, _, _ := setupRaftTest()

	entries := generateNEntries(10, r.lastEntryIndex, r.currentTerm)

	msg := baselineAppendEntryTestMessage(r)

	msg.Entries = entries

	r.Call(msg)
	output := <-r.Ready()
	r.Advance()

	assert(t, nil != output, "Output is not nil")
	baseValidationCycleOutput(t, output, 1, 0, len(entries), 0)

	response := output.SendMessages[0]
	assertEqual(t, "Response is successful", response.Success, true)
	err := validateEntriesAreSequential(defaults.lastEntryIndex, defaults.currentTerm, output.WriteLogEntries)
	if err != nil {
		t.Error(err.Error())
	}

	assertEqual(t, "Last entry index should update", r.lastEntryIndex, defaults.lastEntryIndex+uint64(len(entries)))
}

func TestCallFollowerAppendEntriesApplyEntries(t *testing.T) {
	r, defaults, _, _ := setupRaftTest()

	msga := baselineAppendEntryTestMessage(r)

	appliedDiff := uint64(50)
	r.commitIndex = r.commitIndex - appliedDiff
	r.lastAppliedIndex = r.lastAppliedIndex - appliedDiff
	indexBeforeCall := r.lastAppliedIndex
	termBeforeCall := r.currentTerm

	r.Call(msga)
	output := <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 1, 0, 0, int(appliedDiff))
	sendmsg := output.SendMessages[0]
	assert(t, sendmsg.Success, "Should be message")
	assertEqual(t, "Commit index was updated", r.commitIndex, defaults.commitIndex)
	assertEqual(t, "Applied index was updated", r.lastAppliedIndex, defaults.lastAppliedIndex)
	err := validateEntriesAreSequential(indexBeforeCall, termBeforeCall, output.ApplyEntries)
	assert(t, err == nil, "Apply entries is sequential")
}

func TestCallFollowerPrevoteResponse(t *testing.T) {
	// follower sends prevote success if it's within prevote window (electionTimeout - 5)
	// prevote term assumed to be currentTerm + 1
	// ACCEPT EQUAL TERM
	r, defaults, _, _ := setupRaftTest()

	msg := baselineAppendEntryTestMessage(r)

	msg.Type = MESSAGE_PREVOTE_REQUEST
	msg.PreviousLogIndex = defaults.lastEntryIndex
	msg.PreviousLogTerm = defaults.currentTerm
	msg.CandidateId = 1

	r.electionTimeout = 10

	cycleNTicks(r, 6)

	r.Call(msg)
	output := <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 1, 0, 0, 0)

	prevote := output.SendMessages[0]
	assertEqual(t, "Sends prevote response", prevote.Type.String(), MESSAGE_PREVOTE_RESPONSE.String())
	assertEqual(t, "Prevote is granted for equal term and index", prevote.VoteGranted, true)

	// ACCEPT LATER TERM

	msg.PreviousLogTerm = defaults.currentTerm + 1
	msg.CandidateId = 1
	r.Call(msg)
	output = <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 1, 0, 0, 0)

	prevote = output.SendMessages[0]
	assertEqual(t, "Sends prevote response", prevote.Type.String(), MESSAGE_PREVOTE_RESPONSE.String())
	assertEqual(t, "Prevote is granted for greater term", prevote.VoteGranted, true)

	// REJECT LOWER LOG INDEX
	msg.PreviousLogTerm = defaults.currentTerm
	msg.PreviousLogIndex = defaults.lastEntryIndex - 1
	msg.CandidateId = 1
	r.Call(msg)
	output = <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 1, 0, 0, 0)

	prevote = output.SendMessages[0]
	assertEqual(t, "Sends prevote response", prevote.Type.String(), MESSAGE_PREVOTE_RESPONSE.String())
	assertEqual(t, "Prevote is rejected for lower log index", prevote.VoteGranted, false)

	// REJECT LOWER TERM
	msg.PreviousLogTerm = defaults.currentTerm
	msg.PreviousLogIndex = defaults.lastEntryIndex - 1
	msg.CandidateId = 1

	r.Call(msg)
	output = <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 1, 0, 0, 0)

	prevote = output.SendMessages[0]
	assertEqual(t, "Sends prevote response", prevote.Type.String(), MESSAGE_PREVOTE_RESPONSE.String())
	assertEqual(t, "Prevote is rejected for lower log index", prevote.VoteGranted, false)
}

func TestCallFollowerVote(t *testing.T) {
	// REJECTS LOWER TERM
	r, defaults, _, _ := setupRaftTest()

	novotereq := baselineAppendEntryTestMessage(r)

	novotereq.Type = MESSAGE_VOTE_REQUEST
	novotereq.CandidateId = 999
	novotereq.PreviousLogIndex = defaults.lastEntryIndex
	novotereq.PreviousLogTerm = defaults.currentTerm - 1

	r.Call(novotereq)
	output := <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 1, 0, 0, 0)

	prevote := output.SendMessages[0]
	assertEqual(t, "Sends vote response", prevote.Type.String(), MESSAGE_VOTE_RESPONSE.String())
	assertEqual(t, "Vote is rejected for lower term", prevote.VoteGranted, false)
	assertEqual(t, "Term is not updated", r.currentTerm, defaults.currentTerm)

	// REJECT LOWER INDEX

	novotereq.Type = MESSAGE_VOTE_REQUEST
	novotereq.CandidateId = 999
	novotereq.PreviousLogIndex = defaults.lastEntryIndex - 2
	novotereq.PreviousLogTerm = defaults.currentTerm

	r.Call(novotereq)
	output = <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 1, 0, 0, 0)

	prevote = output.SendMessages[0]
	assertEqual(t, "Sends prevote response", prevote.Type.String(), MESSAGE_VOTE_RESPONSE.String())
	assertEqual(t, "Prevote is rejected for lower index", prevote.VoteGranted, false)
	assertEqual(t, "Term is not updated", r.currentTerm, defaults.currentTerm)

	// GRANT VOTE HIGHER TERM
	votereq := baselineAppendEntryTestMessage(r)
	votereq.CandidateId = 999
	votereq.Type = MESSAGE_VOTE_REQUEST
	votereq.Term = defaults.currentTerm + 1
	r.Call(votereq)
	output = <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 1, 1, 0, 0)

	vote := output.SendMessages[0]
	assertEqual(t, "Sends vote response", vote.Type.String(), MESSAGE_VOTE_RESPONSE.String())
	assertEqual(t, "Vote is granted to higher term", vote.VoteGranted, true)
	assertEqual(t, "Term is updated", r.currentTerm, votereq.Term)
	assertEqual(t, "Leader id is updated", r.leader, votereq.CandidateId)
	assertEqual(t, "Votedfor is updated", r.votedFor, votereq.CandidateId)
	assertEqual(t, "Node must be in follower state", r.currentState.String(), raft_follower.String())
}

func TestTransitionToPrecandidate(t *testing.T) {
	r, _, _, _ := setupRaftTest()

	r.electionTimeout = 5
	r.peers = []uint64{1, 2, 3, 4}

	cycleNTicks(r, 5)
	r.Tick()
	output := <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 4, 0, 0, 0)
	assertEqual(t, "Election elapsed is reset", r.electionElapsed, 0)

	for i, msg := range output.SendMessages {
		assertEqual(t, "Sent prevote request", msg.Type.String(), MESSAGE_PREVOTE_REQUEST.String())
		assertEqual(t, "Sent from precandidate", msg.From, r.id)
		assertEqual(t, "Sent with precandidate id", msg.CandidateId, r.id)
		assertEqual(t, "Sent with last entry index", msg.PreviousLogIndex, r.lastEntryIndex)
		assertEqual(t, "Sent with latest term", msg.Term, r.currentTerm)
		assertEqual(t, "Sent with latest entry term", msg.PreviousLogTerm, r.currentTerm)
		assertEqual(t, "Sent to each peer in order", msg.To, r.peers[i])
	}
}

func TestPrecandidateAcceptsVotesAndTransitions(t *testing.T) {
	r, _, _, _ := setupRaftTest()

	r.peers = []uint64{1, 2, 3, 4}

	grant := genericRaftMessage(MESSAGE_PREVOTE_RESPONSE, 1, r.id)
	grant.Term = r.currentTerm
	grant.VoteGranted = true

	reject := genericRaftMessage(MESSAGE_PREVOTE_RESPONSE, 1, r.id)
	reject.Term = r.currentTerm
	reject.VoteGranted = false

	r.electionTimeout = 5

	cycleNTicks(r, 6)

	assertEqual(t, "Went into precandidate state", r.currentState.String(), raft_precandidate.String())

	r.electionTimeout = 5

	cycleNTicks(r, 6)

	assertEqual(t, "Stays in precandidate state after timeout", r.currentState.String(), raft_precandidate.String())

	for range 5 {
		r.Call(reject)
		<-r.Ready()
		r.Advance()
		assertEqual(t, "Votes are not counted", r.votes, 0)
		assertEqual(t, "Must stay in precandidate state", r.currentState.String(), raft_precandidate.String())
	}

	for range 2 {
		r.Call(grant)
		output := <-r.Ready()
		assert(t, output == nil, "Output should be nil")
		r.Advance()
	}

	r.Call(grant)
	<-r.Ready()
	r.Advance()

	assertEqual(t, "Should go to Candidate status on quorum", r.currentState.String(), raft_candidate.String())
}

func TestPrecandidateStepsDownToFollowerOnHeartbeat(t *testing.T) {
	r, defaults, _, _ := setupRaftTest()

	r.electionTimeout = 5

	cycleNTicks(r, 6)

	assertEqual(t, "In precandidate state", r.currentState.String(), raft_precandidate.String())

	msg := genericRaftMessage(MESSAGE_APPEND, 1, r.id)
	msg.LeaderId = msg.From
	msg.Term = r.currentTerm + 1
	msg.PreviousLogIndex = r.lastEntryIndex
	msg.PreviousLogTerm = r.currentTerm

	r.Call(msg)
	output := <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 1, 1, 0, 0)

	response := output.SendMessages[0]
	assertEqual(t, "Sends append entry response", response.Type.String(), MESSAGE_APPEND_RESPONSE.String())
	assertEqual(t, "Sends success response", response.Success, true)

	update := output.UpdateMetadata[0]
	assertEqual(t, "Updates term", update.CurrentTerm, defaults.currentTerm+1)
	assertEqual(t, "Updates current term", r.currentTerm, update.CurrentTerm)
	assertEqual(t, "Steps down to follower", r.currentState.String(), raft_follower.String())
}

func TestTransitionToCandidate(t *testing.T) {
	r, defaults, _, _ := setupRaftTest()

	r.electionTimeout = 5
	r.peers = []uint64{1, 2, 3, 4}

	r.currentState = raft_precandidate
	r.transitionCandidate()
	go r.loadOutboundToReady()
	output := <-r.Ready()
	r.advance()

	baseValidationCycleOutput(t, output, 4, 1, 0, 0)
	assertEqual(t, "Election elapsed is reset", r.electionElapsed, 0)
	assertEqual(t, "Current state is updated", r.currentState.String(), raft_candidate.String())
	assertEqual(t, "Current term is updated", r.currentTerm, defaults.currentTerm+1)
	assertEqual(t, "Voted for is updated", r.votedFor, r.id)
	assertEqual(t, "Election elapsed is reset", r.electionElapsed, 0)
	assertEqual(t, "Candidate voted for self", r.votes, 1)

	for i, msg := range output.SendMessages {
		assertEqual(t, "Sent vote request", msg.Type.String(), MESSAGE_VOTE_REQUEST.String())
		assertEqual(t, "Sent from candidate", msg.From, r.id)
		assertEqual(t, "Sent with candidate id", msg.CandidateId, r.id)
		assertEqual(t, "Sent with last entry index", msg.PreviousLogIndex, r.lastEntryIndex)
		assertEqual(t, "Sent with latest term", msg.Term, r.currentTerm)
		assertEqual(t, "Sent with latest entry term", msg.PreviousLogTerm, r.lastEntryTerm)
		assertEqual(t, "Sent to each peer in order", msg.To, r.peers[i])
	}
}

func TestCandidateStepsDownWhenBehindTerm(t *testing.T) {
	r, defaults, _, _ := setupRaftTest()

	r.currentState = raft_precandidate
	r.transitionCandidate()
	go r.loadOutboundToReady()
	<-r.Ready()
	r.advance()

	assert(t, r.currentState == raft_candidate && r.electionElapsed == 0 && r.currentTerm == defaults.currentTerm+1, "Baseline candidate established")

	latestTerm := r.currentTerm + 3
	newLeaderId := uint64(1)
	msg := baselineAppendEntryTestMessage(r)
	msg.LeaderId = newLeaderId
	msg.Term = latestTerm

	r.Call(msg)
	output := <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 1, 1, 0, 0)

	// success message
	response := output.SendMessages[0]
	assertEqual(t, "Sent success response", response.Success, true)
	assertEqual(t, "Responds with latest term", response.Term, latestTerm)
	assertEqual(t, "Sent append response type", response.Type.String(), MESSAGE_APPEND_RESPONSE.String())
	// update with votefor and new term
	update := output.UpdateMetadata[0]
	assertEqual(t, "Updates to correct term", update.CurrentTerm, msg.Term)
	// leader updated
	assertEqual(t, "Updated to latest leader", r.leader, newLeaderId)
	// term updated
	assertEqual(t, "Updated to latest term", r.currentTerm, latestTerm)
}

func TestCandidateVotesTransitionToLeader(t *testing.T) {
	r, defaults, _, _ := setupRaftTest()

	r.peers = []uint64{1, 2, 3, 4}

	r.currentState = raft_precandidate
	r.transitionCandidate()
	go r.loadOutboundToReady()
	<-r.Ready()
	r.advance()

	assert(t, r.currentState == raft_candidate && r.electionElapsed == 0 && r.currentTerm == defaults.currentTerm+1, "Baseline candidate established")

	reject := genericRaftMessage(MESSAGE_VOTE_RESPONSE, 1, r.id)
	reject.Term = r.currentTerm
	reject.VoteGranted = false

	r.electionTimeout = 5

	cycleNTicks(r, 6)

	assertEqual(t, "Stayed in candidate state through timeout", r.currentState.String(), raft_candidate.String())

	r.electionTimeout = 5

	cycleNTicks(r, 6)

	assertEqual(t, "Spins in candidate state after timeout", r.currentState.String(), raft_candidate.String())
	assertEqual(t, "Term increases on spin", r.currentTerm, defaults.currentTerm+2)

	for range 5 {
		r.Call(reject)
		<-r.Ready()
		r.Advance()
		assertEqual(t, "Votes are not counted", r.votes, 1)
		assertEqual(t, "Must stay in precandidate state", r.currentState.String(), raft_candidate.String())
	}

	grant := genericRaftMessage(MESSAGE_VOTE_RESPONSE, 1, r.id)
	grant.Term = r.currentTerm
	grant.VoteGranted = true

	for i := range 2 {
		r.Call(grant)
		<-r.Ready()
		r.Advance()
		assertEqual(t, "Votes accumulated correctly", r.votes, uint64(i+2)) // offset for i starting at 0 and self vote
	}

	assertEqual(t, "Should go to Leader state simple majority", r.currentState.String(), raft_leader.String())
}

func TestLeaderStepsDownIfStale(t *testing.T) {
	r, defaults, _, _ := setupRaftTest()

	newLeader := uint64(111)
	r.currentState = raft_leader
	r.call = r.callLeader

	failTerm := r.currentTerm
	fail := baselineAppendEntryTestMessage(r)
	fail.LeaderId = newLeader
	fail.Term = failTerm

	r.Call(fail)
	output := <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 1, 0, 0, 0)

	response := output.SendMessages[0]
	assertEqual(t, "Term should not change", r.currentTerm, defaults.currentTerm)
	assertEqual(t, "Should send failure response", response.Success, false)

	successTerm := r.currentTerm + 3
	success := baselineAppendEntryTestMessage(r)
	success.LeaderId = newLeader
	success.Term = successTerm

	r.Call(success)
	output = <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 1, 1, 0, 0)

	response = output.SendMessages[0]
	update := output.UpdateMetadata[0]
	assertEqual(t, "Should send success message", response.Success, true)
	assertEqual(t, "Should update term in metadata", update.CurrentTerm, successTerm)
	assertEqual(t, "Leader updates", r.leader, newLeader)
	assertEqual(t, "Term updates", r.currentTerm, successTerm)
	assertEqual(t, "Steps down to follower", r.currentState.String(), raft_follower.String())
}

func TestTransitionToLeader(t *testing.T) {
	r, defaults, _, _ := setupRaftTest()

	r.currentState = raft_candidate

	r.transitionLeader()
	go r.loadOutboundToReady()
	output := <-r.Ready()
	r.advance()

	baseValidationCycleOutput(t, output, 4, 0, 1, 0)
	assertEqual(t, "Entry index was incremented", r.lastEntryIndex, defaults.lastEntryIndex+1)

	entry := output.WriteLogEntries[0]
	assertEqual(t, "Correct index on entry", entry.Index, defaults.lastEntryIndex+1)
	assertEqual(t, "Correct term on entry", entry.Term, r.currentTerm)
	for _, msg := range output.SendMessages {
		assertEqual(t, "Sends append msg", msg.Type.String(), MESSAGE_APPEND.String()) // sends append entry type
		assertEqual(t, "Payload is 0 len", len(msg.Entries[0].Payload), 0)
		assertEqual(t, "Term does not change", msg.Term, defaults.currentTerm)
	}
}

func TestLeaderSendsHeartbeatOnTick(t *testing.T) {
	r, _, _, _ := setupRaftTest()

	r.currentState = raft_candidate
	r.transitionLeader()
	r.time = 100
	go r.loadOutboundToReady()
	<-r.Ready()
	r.advance()

	assert(t, r.currentState == raft_leader, "Establish baseline leader")

	startTime := r.time
	for i := range 5 {
		r.Tick()
		output := <-r.Ready()
		r.Advance()

		timeInc := startTime + uint64(i+1)
		assertEqual(t, "Time is incrementing per tick", r.time, timeInc)
		baseValidationCycleOutput(t, output, len(r.peers), 0, 0, 0)
		for j, msg := range output.SendMessages {
			assertEqual(t, "Addressed to a peer", msg.To, r.peers[j])
			assertEqual(t, "Sent append message", msg.Type.String(), MESSAGE_APPEND.String())
			assertEqual(t, "Previous entry has correct term", msg.PreviousLogTerm, r.currentTerm)
			assertEqual(t, "Previous entry correct index", msg.PreviousLogIndex, r.lastEntryIndex)
			assertEqual(t, "Sent with correct commit", msg.LeaderCommit, r.commitIndex)
		}
	}

}

func TestLeaderSendsNewWritesToAllFollowers(t *testing.T) {
	r, defaults, _, _ := setupRaftTest()

	r.currentState = raft_candidate
	r.transitionLeader()
	r.time = 100
	go r.loadOutboundToReady()
	<-r.Ready()
	r.advance()

	rawEntries := [][]byte{}
	rawEntries = append(rawEntries, []byte("one"))
	rawEntries = append(rawEntries, []byte("two"))
	rawEntries = append(rawEntries, []byte("three"))
	rawEntries = append(rawEntries, []byte("four"))
	startIndex := r.lastEntryIndex

	msg := RaftMessage{
		Type:       MESSAGE_NEW_ENTRY,
		RawEntries: rawEntries,
	}

	r.Call(msg)
	output := <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 4, 0, len(rawEntries), 0)
	assertEqual(t, "Last entry index updated", r.lastEntryIndex, startIndex+uint64(len(rawEntries)))

	err := validateEntriesAreSequential(startIndex, defaults.currentTerm, output.WriteLogEntries)
	if err != nil {
		t.Errorf(err.Error())
	}

	for i, msg := range output.SendMessages {
		assertEqual(t, "Addressed to a peer", msg.To, r.peers[i])
		assertEqual(t, "Sent append message", msg.Type.String(), MESSAGE_APPEND.String())
		assertEqual(t, "Previous entry has correct term", msg.PreviousLogTerm, defaults.currentTerm)
		assertEqual(t, "Previous entry correct index", msg.PreviousLogIndex, startIndex)
		assertEqual(t, "Sent with correct commit", msg.LeaderCommit, r.commitIndex)
		for j, e := range msg.Entries {
			assertEqual(t, "Correct entries sent", string(e.Payload), string(rawEntries[j]))
		}
	}
}

func TestLeaderSendsUpdateForCommit(t *testing.T) {
	r, _, _, mlog := setupRaftTest()
	r.currentState = raft_candidate
	r.transitionLeader()
	r.time = 100

	// TODO calling loadOutboundToReady and using internal functions causes race condition in test env, need to find better workaround
	go r.loadOutboundToReady()
	output := <-r.Ready()
	assert(t, nil != output, "output not nil")
	mlog.appendRaftEntries(output.WriteLogEntries)
	r.advance()

	rawEntries := [][]byte{}
	rawEntries = append(rawEntries, []byte("one"))
	rawEntries = append(rawEntries, []byte("two"))
	rawEntries = append(rawEntries, []byte("three"))
	rawEntries = append(rawEntries, []byte("four"))
	startIndex := r.lastEntryIndex
	startApplied := r.lastAppliedIndex

	msg := RaftMessage{
		Type:       MESSAGE_NEW_ENTRY,
		RawEntries: rawEntries,
	}

	r.Call(msg)
	output = <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 4, 0, len(rawEntries), 0)
	assertEqual(t, "Last entry index updated", r.lastEntryIndex, startIndex+uint64(len(rawEntries)))
	assertEqual(t, "Last applied index not updated", r.lastAppliedIndex, startApplied)
	mlog.appendRaftEntries(output.WriteLogEntries)

	response1 := baselineAppendEntryTestMessage(r)
	response1.Type = MESSAGE_APPEND_RESPONSE
	response1.To = r.id
	response1.From = 2

	response2 := baselineAppendEntryTestMessage(r)
	response2.Type = MESSAGE_APPEND_RESPONSE
	response2.To = r.id
	response2.From = 3

	r.Call(response1)
	output = <-r.Ready()
	r.Advance()

	assert(t, nil == output, "First output should be nil")
	r.Call(response2)
	output = <-r.Ready()
	r.Advance()

	// must include initial leader commit in apply entries count
	baseValidationCycleOutput(t, output, 4, 0, 0, len(rawEntries)+1)
}

func TestLeaderCorrectsFollowerThatsBehind(t *testing.T) {
	// set up leader
	r, _, _, mlog := setupRaftTest()
	r.currentState = raft_candidate
	r.transitionLeader()
	r.time = 100

	// TODO calling loadOutboundToReady and using internal functions causes race condition in test env, need to find better workaround
	go r.loadOutboundToReady()
	output := <-r.Ready()
	assert(t, nil != output, "output not nil")
	mlog.appendRaftEntries(output.WriteLogEntries)
	r.advance()
	// make msg append response from follower

	backIndex := uint64(150)
	backTerm := uint64(2)
	msg := baselineAppendEntryTestMessage(r)
	msg.Success = false
	msg.Type = MESSAGE_APPEND_RESPONSE
	msg.From = 1
	msg.To = r.id
	msg.PreviousLogIndex = backIndex
	msg.PreviousLogTerm = backTerm

	r.Call(msg)
	output = <-r.Ready()
	r.Advance()

	baseValidationCycleOutput(t, output, 1, 0, 0, 0)
	msg = output.SendMessages[0]
	expectedIndex, _ := mlog.StartOfTerm(2)
	expectedIndex-- // needs to start at index before no-op
	expectedTerm := backTerm - 1

	assertEqual(t, "Catch up message should start with first message from term", msg.PreviousLogIndex, expectedIndex)
	assertEqual(t, "Catch up should have same previous term", msg.PreviousLogTerm, expectedTerm)
	assertEqual(t, "Catch up sends 100 entries at a time", len(msg.Entries), 100)
	err := validateEntriesAreSequential(expectedIndex, expectedTerm, msg.Entries)
	if err != nil {
		println("validation error")
		t.Error(err.Error())
	}

	// call second response from first catch up message
	// output should have entries starting at the beginning of the term in the follower's response
	// output should have chunks of 100 entries
	// second follower response should have up to last catch up entry
	// leader should send another msg append from follower's last entry until caught up

}

func TestLeaderChecksForQuorumInResponses(t *testing.T) {}
