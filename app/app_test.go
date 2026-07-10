package main

import (
	"testing"
	"time"

	"github.com/samasno/raft-kv/raft"
)

func TestControlLoopElection(t *testing.T) {
	const count = 3
	raftConfigs := testingRaftConfigs(count)
	rpcConfigs := testingRpcConfig(count)

	coordinators := map[uint64]*RaftCoordinator{}
	for i := uint64(1); i <= count; i++ {
		coordinators[i] = NewCoordinator()
	}

	errc := make(chan error, count)
	for i := uint64(1); i <= count; i++ {
		id := i
		rc := coordinators[id]
		sm := newTestStateMachine(id)
		dir := t.TempDir()
		go func() {
			errc <- rc.StartControlLoop(raftConfigs[id], rpcConfigs[id], dir, sm)
		}()
		t.Cleanup(func() { rc.Done() })
	}

	// wait for election; fail fast if any coordinator exits before timeout
	select {
	case err := <-errc:
		t.Fatalf("coordinator exited early: %v", err)
	case <-time.After(10 * time.Second):
	}

	// send all proposals concurrently — non-leaders respond immediately,
	// the leader only responds after followers acknowledge (full commit round-trip)
	type proposalResult struct {
		id   uint64
		resp RaftProposalResponse
	}
	println("at result")
	resultc := make(chan proposalResult, count)
	for i := uint64(1); i <= count; i++ {
		id := i
		resp := make(chan RaftProposalResponse, 1)
		msg := raft.RaftMessage{
			Type:       raft.MessageNewEntry,
			RawEntries: [][]byte{[]byte("probe")},
		}
		go func() {
			coordinators[id].Propose(msg, resp)
			r := <-resp
			resultc <- proposalResult{id, r}
		}()
	}

	// collect all responses; timeout must cover full follower acknowledgement round-trip
	leaderCount := 0
	timeout := time.After(2 * time.Second)
	for received := 0; received < count; {
		select {
		case r := <-resultc:
			received++
			if r.resp.Success {
				leaderCount++
			}
		case err := <-errc:
			t.Fatalf("coordinator error during proposals: %v", err)
		case <-timeout:
			t.Logf("timed out after %d/%d responses", received, count)
			goto done
		}
	}

done:
	if leaderCount != 1 {
		t.Errorf("expected exactly 1 leader, got %d", leaderCount)
	}
}
