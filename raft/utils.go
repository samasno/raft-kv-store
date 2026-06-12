package raft

import (
	"fmt"
	"math/rand"
)

// random tick count for election timeout
func randomTimeout(min, max int) uint64 {
	r := rand.Intn(max - min)
	return uint64(r + min)
}

func validateEntriesAreSequential(prevIndex, prevTerm uint64, entries []RaftEntry) error {
	i := prevIndex
	t := prevTerm
	for _, entry := range entries {
		if i+1 != entry.Index {
			return fmt.Errorf("Non-sequential index found at index:%d term: %d", entry.Index, entry.Term)
		}

		if entry.Term < t {
			return fmt.Errorf("Nonmonotonic term found at index:%d term: %d", entry.Index, entry.Term)
		}

		i = entry.Index
		t = entry.Term
	}

	return nil
}
