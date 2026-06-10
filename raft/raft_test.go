package raft

import "testing"

func TestFollowerTicks(t *testing.T) {
	id := uint64(9)
	votedFor := uint64(7)
	currentTerm := uint64(22)
	lastLogIndex := uint64(77)
	startTime := uint64(100)

	startingEntry := RaftEntry{
		Index: lastLogIndex,
		Term:  currentTerm,
	}

	startingLog := []RaftEntry{startingEntry}

	mlog := newInMemoryLogfile(startingLog)

	conf := RaftConfig{id}

	md := newInMemoryMetadataFile(votedFor, currentTerm)

	r, err := NewRaftInstance(md, mlog, conf)
	if err != nil {
		t.Fatal(err.Error())
	}

	r.time = startTime
	r.electionTimeout = 4

	for i := uint64(1); i <= 4; i++ {
		r.Tick()
		expectedTime := startTime + i
		assertEqual(t, "Time", r.time, expectedTime)
		assertEqual(t, "State", raftStateString(r.currentState), raftStateString(raft_follower))
		assertEqual(t, "Election Elapsed", r.electionElapsed, i)
	}

	r.Tick()

	assertEqual(t, "State", raftStateString(r.currentState), raftStateString(raft_precandidate))
	assertEqual(t, "Election Elapsed", r.electionElapsed, 0)
	assertEqual(t, "Voted For", r.votedFor, votedFor)
	assertEqual(t, "Current Term", r.currentTerm, currentTerm)
	assertEqual(t, "Last Log Index", r.lastEntryIndex, lastLogIndex)
	assert(t, r.electionTimeout > 9, "Election Timeout: Expected to be greater than 9")
}

func TestCallFollowerHeartBeatSameTerm(t *testing.T) {
	// prep raft instance
	// arbitrary ticks
	// call a heartbeat with same leader - should update leader
	// update commit w/ output
}

func TestCallFollowerHeartBeatOldTerm(t *testing.T) {

}
func TestCallFollowerAppendEntriesWhenValid(t *testing.T) {}

func TestCallFollowerAppendEntriesWrongIndex(t *testing.T) {}

func TestCallFollowerPrevote(t *testing.T) {}

func TestCallFollowerVote(t *testing.T) {}
