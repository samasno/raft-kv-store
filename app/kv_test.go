package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/samasno/raft-kv-store/raft"
)

// mockLog implements LogReader for tests.
type mockLog struct {
	entries []raft.RaftEntry
}

func (m *mockLog) GetEntries(start, end uint64) ([]raft.RaftEntry, error) {
	var result []raft.RaftEntry
	for _, e := range m.entries {
		if e.Index >= start && e.Index <= end {
			result = append(result, e)
		}
	}
	return result, nil
}

func newTestKV(t *testing.T, log LogReader) *KVMap {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "checkpoint")
	if err != nil {
		t.Fatalf("create checkpoint: %v", err)
	}
	t.Cleanup(func() { f.Close() })
	return &KVMap{
		values:     map[string]string{},
		mtx:        &sync.Mutex{},
		checkpoint: f,
		log:        log,
	}
}

func makeEntry(index uint64, cmd Command) raft.RaftEntry {
	payload, _ := json.Marshal(cmd)
	return raft.RaftEntry{Index: index, Term: 1, Payload: payload}
}

func TestKVSetGet(t *testing.T) {
	kv := newTestKV(t, nil)

	kv.set("foo", "bar")
	if got := kv.get("foo"); got != "bar" {
		t.Errorf("get(foo) = %q, want %q", got, "bar")
	}
}

func TestKVGetMissingKey(t *testing.T) {
	kv := newTestKV(t, nil)
	if got := kv.get("missing"); got != "" {
		t.Errorf("get(missing) = %q, want empty", got)
	}
}

func TestKVSetOverwrite(t *testing.T) {
	kv := newTestKV(t, nil)
	kv.set("k", "first")
	kv.set("k", "second")
	if got := kv.get("k"); got != "second" {
		t.Errorf("after overwrite get(k) = %q, want %q", got, "second")
	}
}

func TestKVDelete(t *testing.T) {
	kv := newTestKV(t, nil)
	kv.set("k", "v")
	kv.delete("k")
	if got := kv.get("k"); got != "" {
		t.Errorf("after delete get(k) = %q, want empty", got)
	}
}

func TestApplySet(t *testing.T) {
	kv := newTestKV(t, nil)
	entries := []raft.RaftEntry{
		makeEntry(1, Command{Op: SET, Key: "a", Value: "1"}),
		makeEntry(2, Command{Op: SET, Key: "b", Value: "2"}),
	}
	if err := kv.apply(entries); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := kv.get("a"); got != "1" {
		t.Errorf("get(a) = %q, want %q", got, "1")
	}
	if got := kv.get("b"); got != "2" {
		t.Errorf("get(b) = %q, want %q", got, "2")
	}
	if kv.lastApplied != 2 {
		t.Errorf("lastApplied = %d, want 2", kv.lastApplied)
	}
}

func TestApplyDelete(t *testing.T) {
	kv := newTestKV(t, nil)
	kv.set("foo", "bar")
	entries := []raft.RaftEntry{
		makeEntry(1, Command{Op: DEL, Key: "foo"}),
	}
	if err := kv.apply(entries); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := kv.get("foo"); got != "" {
		t.Errorf("after DEL get(foo) = %q, want empty", got)
	}
}

func TestApplyGetDoesNotMutateState(t *testing.T) {
	kv := newTestKV(t, nil)
	kv.set("k", "v")
	entries := []raft.RaftEntry{
		makeEntry(1, Command{Op: GET, Key: "k"}),
	}
	if err := kv.apply(entries); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := kv.get("k"); got != "v" {
		t.Errorf("GET entry mutated value: got %q, want %q", got, "v")
	}
}

func TestApplyMixedCommands(t *testing.T) {
	kv := newTestKV(t, nil)
	entries := []raft.RaftEntry{
		makeEntry(1, Command{Op: SET, Key: "x", Value: "10"}),
		makeEntry(2, Command{Op: SET, Key: "y", Value: "20"}),
		makeEntry(3, Command{Op: DEL, Key: "x"}),
		makeEntry(4, Command{Op: SET, Key: "y", Value: "99"}),
	}
	if err := kv.apply(entries); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := kv.get("x"); got != "" {
		t.Errorf("deleted key x still present: %q", got)
	}
	if got := kv.get("y"); got != "99" {
		t.Errorf("get(y) = %q, want %q", got, "99")
	}
	if kv.lastApplied != 4 {
		t.Errorf("lastApplied = %d, want 4", kv.lastApplied)
	}
}

func TestApplyEmptyEntriesResetsCheckpoint(t *testing.T) {
	kv := newTestKV(t, nil)
	_ = kv.apply([]raft.RaftEntry{makeEntry(5, Command{Op: SET, Key: "k", Value: "v"})})
	if kv.lastApplied != 5 {
		t.Fatalf("setup: lastApplied = %d, want 5", kv.lastApplied)
	}
}

func TestReplay(t *testing.T) {
	log := &mockLog{
		entries: []raft.RaftEntry{
			makeEntry(1, Command{Op: SET, Key: "a", Value: "alpha"}),
			makeEntry(2, Command{Op: SET, Key: "b", Value: "beta"}),
			makeEntry(3, Command{Op: DEL, Key: "a"}),
			makeEntry(4, Command{Op: SET, Key: "c", Value: "gamma"}),
		},
	}
	kv := newTestKV(t, log)

	if err := kv.Replay(4); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if got := kv.get("a"); got != "" {
		t.Errorf("deleted key a = %q, want empty", got)
	}
	if got := kv.get("b"); got != "beta" {
		t.Errorf("get(b) = %q, want %q", got, "beta")
	}
	if got := kv.get("c"); got != "gamma" {
		t.Errorf("get(c) = %q, want %q", got, "gamma")
	}
}

// TestReplayChunking verifies that logs larger than one chunk (100 entries) are
// fully replayed across multiple GetEntries calls.
func TestReplayChunking(t *testing.T) {
	const total = 250
	entries := make([]raft.RaftEntry, total)
	for i := range entries {
		key := fmt.Sprintf("k%d", i)
		entries[i] = makeEntry(uint64(i+1), Command{Op: SET, Key: key, Value: key})
	}
	log := &mockLog{entries: entries}
	kv := newTestKV(t, log)

	if err := kv.Replay(total); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	for _, i := range []int{0, 99, 149, 249} {
		key := fmt.Sprintf("k%d", i)
		if got := kv.get(key); got != key {
			t.Errorf("get(%s) = %q, want %q", key, got, key)
		}
	}
}

func TestReplayPartialLog(t *testing.T) {
	log := &mockLog{
		entries: []raft.RaftEntry{
			makeEntry(1, Command{Op: SET, Key: "a", Value: "1"}),
			makeEntry(2, Command{Op: SET, Key: "b", Value: "2"}),
			makeEntry(3, Command{Op: SET, Key: "c", Value: "3"}),
		},
	}
	kv := newTestKV(t, log)

	if err := kv.Replay(2); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if got := kv.get("c"); got != "" {
		t.Errorf("entry past end was applied: get(c) = %q", got)
	}
	if got := kv.get("b"); got != "2" {
		t.Errorf("get(b) = %q, want %q", got, "2")
	}
}
