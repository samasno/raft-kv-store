package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/samasno/raft-kv-store/raft"
	"github.com/samasno/raft-kv-store/rpc"
)

// --- mocks ---

type mockRaftClient struct {
	resp RaftProposalResponse
}

func (m *mockRaftClient) Propose(_ raft.RaftMessage, c chan RaftProposalResponse) {
	go func() { c <- m.resp }()
}

type mockKVStore struct {
	values map[string]string
}

func (m *mockKVStore) Get(key string) string                { return m.values[key] }
func (m *mockKVStore) ApplyRecord(_ []raft.RaftEntry) error { return nil }

// handlerServer builds a KVServer wired to httptest for handler unit tests.
func handlerServer(rc RaftClient, kv KVStore, peers map[uint64]string) *KVServer {
	return NewKVServer(kv, rc, "", peers)
}

func encodeBody(t *testing.T, cmd Command) *bytes.Reader {
	t.Helper()
	b, _ := json.Marshal(cmd)
	return bytes.NewReader(b)
}

func decodeResponse(t *testing.T, body []byte) Response {
	t.Helper()
	var r Response
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("decode response: %v (body: %s)", err, body)
	}
	return r
}

// --- sendCommandToPeer unit tests ---

func TestSendCommandToPeerNoPeer(t *testing.T) {
	ks := handlerServer(&mockRaftClient{}, &mockKVStore{}, map[uint64]string{})
	_, err := ks.sendCommandToPeer(99, Command{Op: SET, Key: "k", Value: "v"})
	if err == nil {
		t.Error("expected error for missing peer, got nil")
	}
}

// --- POST /record (HandleRequest) tests ---

func TestHandleRequestSet(t *testing.T) {
	ks := handlerServer(&mockRaftClient{resp: RaftProposalResponse{Success: true}}, &mockKVStore{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/record", encodeBody(t, Command{Op: SET, Key: "k", Value: "v"}))
	w := httptest.NewRecorder()
	ks.HandleRequest(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body)
	}
	resp := decodeResponse(t, w.Body.Bytes())
	if !resp.Success {
		t.Error("response.Success is false")
	}
}

func TestHandleRequestGet(t *testing.T) {
	kv := &mockKVStore{values: map[string]string{"x": "42"}}
	ks := handlerServer(&mockRaftClient{}, kv, nil)
	req := httptest.NewRequest(http.MethodPost, "/record", encodeBody(t, Command{Op: GET, Key: "x"}))
	w := httptest.NewRecorder()
	ks.HandleRequest(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	resp := decodeResponse(t, w.Body.Bytes())
	if resp.Value != "42" {
		t.Errorf("get(x) = %q, want %q", resp.Value, "42")
	}
}

func TestHandleRequestGetMissingKey(t *testing.T) {
	ks := handlerServer(&mockRaftClient{}, &mockKVStore{values: map[string]string{}}, nil)
	req := httptest.NewRequest(http.MethodPost, "/record", encodeBody(t, Command{Op: GET, Key: "missing"}))
	w := httptest.NewRecorder()
	ks.HandleRequest(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	resp := decodeResponse(t, w.Body.Bytes())
	if resp.Value != "" {
		t.Errorf("missing key returned %q, want empty", resp.Value)
	}
}

func TestHandleRequestDel(t *testing.T) {
	ks := handlerServer(&mockRaftClient{resp: RaftProposalResponse{Success: true}}, &mockKVStore{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/record", encodeBody(t, Command{Op: DEL, Key: "k"}))
	w := httptest.NewRecorder()
	ks.HandleRequest(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body)
	}
	resp := decodeResponse(t, w.Body.Bytes())
	if !resp.Success {
		t.Error("response.Success is false")
	}
}

func TestHandleRequestInvalidOp(t *testing.T) {
	ks := handlerServer(&mockRaftClient{}, &mockKVStore{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/record", encodeBody(t, Command{Op: "INVALID", Key: "k"}))
	w := httptest.NewRecorder()
	ks.HandleRequest(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for unknown op, got %d", w.Code)
	}
}

// TestHandleRequestOpCaseInsensitive verifies that lowercase op values are normalized.
func TestHandleRequestOpCaseInsensitive(t *testing.T) {
	ks := handlerServer(&mockRaftClient{resp: RaftProposalResponse{Success: true}}, &mockKVStore{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/record", encodeBody(t, Command{Op: "set", Key: "k", Value: "v"}))
	w := httptest.NewRecorder()
	ks.HandleRequest(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for lowercase op (handleCommand normalizes), got %d", w.Code)
	}
}

// TestHandleRequestProxiesToLeader verifies that a follower receiving POST proxies
// SET to the leader via POST on the leader's /record endpoint.
func TestHandleRequestProxiesToLeader(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected leader to receive POST, got %s", r.Method)
		}
		json.NewEncoder(w).Encode(Response{Success: true, Key: "k"})
	}))
	defer leader.Close()

	peers := map[uint64]string{2: leader.URL}
	ks := handlerServer(
		&mockRaftClient{resp: RaftProposalResponse{Success: false, LeaderId: 2}},
		&mockKVStore{},
		peers,
	)
	req := httptest.NewRequest(http.MethodPost, "/record", encodeBody(t, Command{Op: SET, Key: "k", Value: "v"}))
	w := httptest.NewRecorder()
	ks.HandleRequest(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 after proxy, got %d: %s", w.Code, w.Body)
	}
	resp := decodeResponse(t, w.Body.Bytes())
	if !resp.Success {
		t.Error("proxied response.Success is false")
	}
}

// --- integration test helpers ---

func kvTestRaftConfigs(count int) map[uint64]*raft.RaftConfig {
	configs := map[uint64]*raft.RaftConfig{}
	for i := uint64(1); i <= uint64(count); i++ {
		peers := []uint64{}
		for j := uint64(1); j <= uint64(count); j++ {
			if j != i {
				peers = append(peers, j)
			}
		}
		configs[i] = &raft.RaftConfig{Id: i, Peers: peers}
	}
	return configs
}

func kvTestRpcConfigs(count int) map[uint64]*rpc.RaftServerConfig {
	peers := map[uint64]rpc.Peer{}
	for i := uint64(1); i <= uint64(count); i++ {
		peers[i] = rpc.Peer{Url: fmt.Sprintf("http://127.0.0.1:804%d", i)}
	}
	configs := map[uint64]*rpc.RaftServerConfig{}
	for i := uint64(1); i <= uint64(count); i++ {
		configs[i] = &rpc.RaftServerConfig{
			Id:    i,
			Addr:  strings.TrimPrefix(peers[i].Url, "http://"),
			Peers: peers,
		}
	}
	return configs
}

// kvTestKVAddrs returns KVServer HTTP addresses (separate port range from Raft RPC).
func kvTestKVAddrs(count int) map[uint64]string {
	addrs := map[uint64]string{}
	for i := uint64(1); i <= uint64(count); i++ {
		addrs[i] = fmt.Sprintf("http://127.0.0.1:904%d", i)
	}
	return addrs
}

func doKVRequest(baseURL string, cmd Command) (Response, int, error) {
	body, _ := json.Marshal(cmd)
	req, err := http.NewRequest(http.MethodPost, baseURL+baseRecordPath, bytes.NewReader(body))
	if err != nil {
		return Response{}, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return Response{}, 0, err
	}
	defer res.Body.Close()
	var r Response
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return Response{}, res.StatusCode, fmt.Errorf("decode (status %d): %v", res.StatusCode, err)
	}
	return r, res.StatusCode, nil
}

// --- 3-node cluster integration test ---

func TestKVServerCluster(t *testing.T) {
	const count = 3

	raftConfigs := kvTestRaftConfigs(count)
	rpcConfigs := kvTestRpcConfigs(count)
	kvAddrs := kvTestKVAddrs(count)

	peerMap := map[uint64]string{}
	for id, addr := range kvAddrs {
		peerMap[id] = addr
	}

	type testNode struct {
		coord  *RaftCoordinator
		kvAddr string
	}
	nodes := map[uint64]*testNode{}

	errc := make(chan error, count*2)

	for i := uint64(1); i <= count; i++ {
		id := i
		coord := NewCoordinator()
		dir := t.TempDir()

		kv, err := NewKvMap(filepath.Join(dir, "checkpoint"), nil)
		if err != nil {
			t.Fatalf("node %d: NewKvMap: %v", id, err)
		}

		kvAddr := strings.TrimPrefix(kvAddrs[id], "http://")
		server := NewKVServer(kv, coord, kvAddr, peerMap)
		nodes[id] = &testNode{coord: coord, kvAddr: kvAddrs[id]}

		go func() { errc <- coord.StartControlLoop(raftConfigs[id], rpcConfigs[id], dir, kv) }()
		t.Cleanup(func() { coord.Done() })

		serverDone := server.Run()
		go func() {
			if err := <-serverDone; err != nil {
				errc <- err
			}
		}()
		t.Cleanup(func() { server.Close() })
	}

	// Wait for election. 15s to stay stable when other cluster tests precede this one.
	select {
	case err := <-errc:
		t.Fatalf("node exited early during election: %v", err)
	case <-time.After(15 * time.Second):
	}

	// Phase 1: discover leader via coordinator Propose (not via HTTP, which returns
	// success from followers too when they proxy). Only the leader's coordinator
	// returns Success=true directly.
	type probeResult struct {
		id   uint64
		resp RaftProposalResponse
	}
	probec := make(chan probeResult, count)
	probePayload, _ := json.Marshal(Command{Op: SET, Key: "_probe", Value: "1"})
	for id := range nodes {
		id := id
		rc := make(chan RaftProposalResponse, 1)
		go func() {
			nodes[id].coord.Propose(raft.RaftMessage{
				Type:       raft.MessageNewEntry,
				RawEntries: [][]byte{probePayload},
			}, rc)
			probec <- probeResult{id, <-rc}
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
			t.Fatalf("node error during leader discovery: %v", err)
		case <-probeTimeout:
			t.Fatalf("timed out waiting for leader discovery")
		}
	}
	if leaderID == 0 {
		t.Fatalf("could not identify a leader")
	}
	leaderAddr := nodes[leaderID].kvAddr

	// Phase 2: SET entries via the leader's KVServer.
	entries := []struct{ key, value string }{
		{"alpha", "one"},
		{"beta", "two"},
		{"gamma", "three"},
	}
	for _, e := range entries {
		resp, status, err := doKVRequest(leaderAddr, Command{Op: SET, Key: e.key, Value: e.value})
		if err != nil {
			t.Fatalf("SET %s: %v", e.key, err)
		}
		if status != http.StatusOK || !resp.Success {
			t.Fatalf("SET %s: status=%d success=%v", e.key, status, resp.Success)
		}
	}

	// Phase 3: GET each key from the leader and verify.
	for _, e := range entries {
		resp, _, err := doKVRequest(leaderAddr, Command{Op: GET, Key: e.key})
		if err != nil {
			t.Errorf("GET %s: %v", e.key, err)
			continue
		}
		if resp.Value != e.value {
			t.Errorf("GET %s = %q, want %q", e.key, resp.Value, e.value)
		}
	}

	// Phase 4: DEL one key and verify it's gone.
	_, status, err := doKVRequest(leaderAddr, Command{Op: DEL, Key: "alpha"})
	if err != nil || status != http.StatusOK {
		t.Fatalf("DEL alpha: err=%v status=%d", err, status)
	}
	resp, _, err := doKVRequest(leaderAddr, Command{Op: GET, Key: "alpha"})
	if err != nil {
		t.Fatalf("GET after DEL: %v", err)
	}
	if resp.Value != "" {
		t.Errorf("after DEL, GET alpha = %q, want empty", resp.Value)
	}

	// Phase 5: GET from a follower — entries replicate within a few heartbeats.
	time.Sleep(500 * time.Millisecond)
	for id, node := range nodes {
		if id == leaderID {
			continue
		}
		resp, _, err := doKVRequest(node.kvAddr, Command{Op: GET, Key: "beta"})
		if err != nil {
			t.Errorf("GET beta from follower %d: %v", id, err)
			continue
		}
		if resp.Value != "two" {
			t.Errorf("follower %d: GET beta = %q, want %q", id, resp.Value, "two")
		}
		break
	}
}
