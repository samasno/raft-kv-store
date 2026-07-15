package main

import (
	"testing"
	"time"

	"github.com/samasno/raft-kv-store/raft"
)

func TestControlLoopElection(t *testing.T) {
	const count = 3
	const numProposals = 5

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

	// wait for election; fail fast if any coordinator exits
	select {
	case err := <-errc:
		t.Fatalf("coordinator exited early: %v", err)
	case <-time.After(10 * time.Second):
	}

	// phase 1: discover the leader by probing all nodes concurrently
	type probeResult struct {
		id   uint64
		resp RaftProposalResponse
	}
	probec := make(chan probeResult, count)
	for i := uint64(1); i <= count; i++ {
		id := i
		resp := make(chan RaftProposalResponse, 1)
		go func() {
			coordinators[id].Propose(raft.RaftMessage{
				Type:       raft.MessageNewEntry,
				RawEntries: [][]byte{[]byte("probe")},
			}, resp)
			probec <- probeResult{id, <-resp}
		}()
	}

	var leaderID uint64
	probeTimeout := time.After(5 * time.Second)
	for received := 0; received < count; {
		select {
		case r := <-probec:
			received++
			if r.resp.Success {
				leaderID = r.id
			} else if leaderID == 0 {
				leaderID = r.resp.LeaderId
			}
		case err := <-errc:
			t.Fatalf("coordinator error during leader discovery: %v", err)
		case <-probeTimeout:
			t.Fatalf("timed out waiting for leader discovery")
		}
	}
	if leaderID == 0 {
		t.Fatalf("could not identify a leader")
	}

	// phase 2: send numProposals to the identified leader
	resultc := make(chan RaftProposalResponse, numProposals)
	for i := 0; i < numProposals; i++ {
		resp := make(chan RaftProposalResponse, 1)
		go func() {
			coordinators[leaderID].Propose(raft.RaftMessage{
				Type:       raft.MessageNewEntry,
				RawEntries: [][]byte{[]byte("entry")},
			}, resp)
			resultc <- <-resp
		}()
	}

	successes := 0
	proposalTimeout := time.After(10 * time.Second)
	for received := 0; received < numProposals; {
		select {
		case r := <-resultc:
			received++
			if r.Success {
				successes++
			}
		case err := <-errc:
			t.Fatalf("coordinator error during proposals: %v", err)
		case <-proposalTimeout:
			t.Fatalf("timed out after %d/%d proposals", received, numProposals)
		}
	}

	if successes != numProposals {
		t.Errorf("expected %d successful proposals, got %d", numProposals, successes)
	}
}
