package raft

import "testing"

func TestFollowerHandleMessage(t *testing.T) {
	r := NewRaft(RaftConfig{})
	r.Call(RaftMessage{Type: MSG_APPEND})
	r.Tick()
	r.Advance()
}
