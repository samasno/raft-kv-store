package raft

import "testing"

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

	assertEqual(t, "Election elapsed to increase", r.electionElapsed, uint64(n+1))
	baseValidationCycleOutput(t, output, 1, 0, 0, 0)

	failure := output.SendMessages[0]
	assertEqual(t, "Expect failure response", failure.Success, false)
	assertEqual(t, "Correct TO in response", failure.To, r.leader)
	assertEqual(t, "Got RESPONSE type message", failure.Type.String(), MESSAGE_APPEND_RESPONSE.String())
	assertEqual(t, "Correct FROM in response", failure.From, r.id)
	assertEqual(t, "Expected current term", failure.Term, r.currentTerm)
}

func TestCallFollowerHeartbeatLaterTermLeader(t *testing.T) {
	r, _, _, mlog := setupRaftTest()
	msg := baselineAppendEntryTestMessage(r, mlog)
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
	r, _, _, mlog := setupRaftTest()

	msga := baselineAppendEntryTestMessage(r, mlog)

	r.Call(msga)
	output := <-r.Ready()
	r.Advance()

	baseline := output.SendMessages[0]
	assertEqual(t, "Baseline success", baseline.Success, true)

	msgb := baselineAppendEntryTestMessage(r, mlog)
	msgb.PreviousLogIndex = 111111

	r.Call(msgb)
	output = <-r.Ready()
	r.Advance()

	indexFail := output.SendMessages[0]
	assertEqual(t, "Incorrect index fails", indexFail.Success, false)

	msgc := baselineAppendEntryTestMessage(r, mlog)
	msgc.PreviousLogTerm = 111111

	r.Call(msgc)
	output = <-r.Ready()
	r.Advance()

	termFail := output.SendMessages[0]
	assertEqual(t, "Incorrect term fails", termFail.Success, false)
}

func TestCallFollowerAppendEntriesWriteEntries(t *testing.T) {
	r, defaults, _, mlog := setupRaftTest()

	entries := generateNEntries(10, r.lastEntryIndex, r.currentTerm)

	msg := baselineAppendEntryTestMessage(r, mlog)

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
	r, defaults, _, mlog := setupRaftTest()
	msga := baselineAppendEntryTestMessage(r, mlog)

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

func TestCallFollowerPrevoteResponse(t *testing.T) {}

func TestCallFollowerVote(t *testing.T) {}
