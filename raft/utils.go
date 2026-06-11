package raft

import "math/rand"

// random tick count for election timeout
func randomTimeout(min, max int) uint64 {
	r := rand.Intn(max - min)
	return uint64(r + min)
}
