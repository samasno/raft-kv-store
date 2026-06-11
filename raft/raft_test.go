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

func TestCallFollowerHeartBeatSameTerm(t *testing.T) {
	// prep raft instance
	// r, defaults, mdata, mlog := setupRaftTest()
	// arbitrary ticks less than 10
	// for
	// call a heartbeat with same leader - should update leader
	// update commit w/ output
}

func TestCallFollowerHeartBeatOldTerm(t *testing.T) {

}
func TestCallFollowerAppendEntriesWhenValid(t *testing.T) {}

func TestCallFollowerAppendEntriesWrongIndex(t *testing.T) {}

func TestCallFollowerPrevote(t *testing.T) {}

func TestCallFollowerVote(t *testing.T) {}
