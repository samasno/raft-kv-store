package rpc

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/samasno/raft-kv/raft"
)

func TestServerRuns(t *testing.T) {

	receivec := make(chan raft.RaftMessage)

	config := RaftServerConfig{
		Id:    1,
		Peers: nil,
		Addr:  "0.0.0.0:8080",
	}

	rpc, _ := NewRaftRPC(receivec, config)
	rpc.Run()

	append := raft.RaftMessage{
		To:   1,
		From: 2,
		Type: raft.MessageAppend,
	}

	vote := raft.RaftMessage{
		To:   1,
		From: 2,
		Type: raft.MessageVoteResponse,
	}

	replyChan := make(chan raft.RaftMessage)

	go func(in, out chan raft.RaftMessage) {
		msg := <-in
		out <- msg
		msg = <-in
		out <- msg
	}(receivec, replyChan)

	appendJson, err := json.Marshal(append)
	if err != nil {
		t.Error(err.Error())
	}

	voteJson, err := json.Marshal(vote)
	if err != nil {
		t.Error(err.Error())
	}

	resp, err := http.Post("http://localhost:8080/append-entries", jsonContentType, bytes.NewBuffer(appendJson))
	if err != nil {
		t.Error(err.Error())
	}

	msg := <-replyChan
	if msg.Type != append.Type {
		t.Errorf("Append expected type %s got %s", append.Type.String(), msg.Type.String())
	}

	if msg.From != append.From {
		t.Errorf("Append msg expected from %d got %d", append.From, msg.From)
	}

	if http.StatusOK != resp.StatusCode {
		t.Errorf("Expected Ok status, got %d", resp.StatusCode)
	}

	resp, err = http.Post("http://localhost:8080/request-vote", jsonContentType, bytes.NewBuffer(voteJson))
	if err != nil {
		t.Error(err.Error())
	}

	msg = <-replyChan
	if msg.Type != vote.Type {
		t.Errorf("Vote expected type %s got %s", vote.Type.String(), msg.Type.String())
	}

	if msg.From != vote.From {
		t.Errorf("Vote msg expected from %d got %d", vote.From, msg.From)
	}

	if http.StatusOK != resp.StatusCode {
		t.Errorf("Expected Ok status, got %d", resp.StatusCode)
	}

	rpc.Close()
}

func TestClientSendsMessages(t *testing.T) {
	receiveA := make(chan raft.RaftMessage)
	receiveB := make(chan raft.RaftMessage)

	peers := map[uint64]Peer{}

	peers[1] = Peer{
		Url: "http://localhost:8080",
	}

	peers[2] = Peer{
		Url: "http://localhost:8081",
	}

	configA := RaftServerConfig{
		Addr:  "0.0.0.0:8080",
		Peers: peers,
		Id:    1,
	}

	configB := RaftServerConfig{
		Addr:  "0.0.0.0:8081",
		Peers: peers,
		Id:    1,
	}

	rpcA, _ := NewRaftRPC(receiveA, configA)
	rpcB, _ := NewRaftRPC(receiveB, configB)

	rpcA.Run()
	rpcB.Run()

	replyChanA := make(chan raft.RaftMessage)

	go func(in, out chan raft.RaftMessage) {
		for msg := range in {
			out <- msg
		}
	}(receiveA, replyChanA)

	append := raft.RaftMessage{
		To:   1,
		From: 2,
		Type: raft.MessageAppend,
	}

	vote := raft.RaftMessage{
		To:   1,
		From: 2,
		Type: raft.MessageVoteRequest,
	}

	for range 10 {
		err := rpcB.SendMessage(append)
		if err != nil {
			t.Error(err.Error())
		}

		sentMsg := <-replyChanA

		if sentMsg.To != configA.Id {
			t.Errorf("Expected msg addressed to %d got %d", configA.Id, sentMsg.To)
		}

		err = rpcB.SendMessage(vote)
		if err != nil {
			t.Error(err.Error())
		}

		sentMsg = <-replyChanA

		if sentMsg.To != configA.Id {
			t.Errorf("Expected msg addressed to %d got %d", configA.Id, sentMsg.To)
		}
	}

	rpcA.Close()
	rpcB.Close()
}
