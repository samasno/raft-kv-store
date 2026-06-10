package raft

import "math/rand"

// random tick count for election timeout
func randomTimeout(min, max int) uint64 {
	r := rand.Intn(max - min)
	return uint64(r + min)
}

func raftStateString(s raftState) string {
	switch s {
	case raft_follower:
		return "Follower"
	case raft_precandidate:
		return "Precandidate"
	case raft_candidate:
		return "Candidate"
	case raft_leader:
		return "Leader"
	}

	return "Invalid State"
}
