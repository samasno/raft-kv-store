package main

import (
	"fmt"
	"time"

	"github.com/samasno/raft-kv/raft"
	"github.com/samasno/raft-kv/rpc"
	"github.com/samasno/raft-kv/storage"
)

type RaftError uint8

const (
	ErrorNotLeader RaftError = iota
	ErrorFailedValidation
)

type RaftClient interface {
	Propose(raft.RaftMessage, chan raft.RaftMessage) chan raft.RaftMessage
}

type StateMachine interface {
	Apply([]raft.RaftEntry) error
}

type RaftCoordinator struct {
	// mutliplex channels for raft run loop
	proposalc chan RaftProposal
	messagec  chan raft.RaftMessage
	donec     chan struct{}

	// output handlers
	logfile      *storage.LogFile
	metadatafile *storage.MetadataFile
	rpc          *rpc.RPC
	raft         *raft.Raft

	stateMachine StateMachine
}

type RaftProposal struct {
	Message raft.RaftMessage
	Error   chan error
}

func (rc *RaftCoordinator) StartControlLoop(raftConfig *raft.RaftConfig, rpcConfig *rpc.RaftServerConfig, storageDir string) error {
	var err error

	rc.logfile, err = storage.OpenLogFile(storageDir)
	if err != nil {
		return err
	}
	defer rc.logfile.Close()

	rc.metadatafile, err = storage.OpenMetadataFile(storageDir)
	if err != nil {
		return err
	}
	defer rc.metadatafile.Close()

	rc.rpc, err = rpc.NewRaftRPC(rc.messagec, *rpcConfig)
	if err != nil {
		return err
	}
	defer rc.rpc.Close()

	rc.raft, err = raft.NewRaftInstance(rc.metadatafile, rc.logfile, *raftConfig)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(time.Duration(time.Millisecond) * 100)
	exit := false
	for {
		select {
		case msg := <-rc.messagec:
			rc.handleMessage(msg)
		case pr := <-rc.proposalc:
			rc.handleProposal(pr)
		case <-rc.donec:
			rc.raft.Done()
			exit = true
		case <-ticker.C:
			rc.raft.Tick()
		}
		if exit {
			break
		}
	}

	// close log, meta, rpc
	return nil
}

func (rc *RaftCoordinator) handleMessage(msg raft.RaftMessage) {
	rc.raft.Call(msg)
	output := <-rc.raft.Ready()

	var err error
	for _, update := range output.UpdateMetadata {
		err = rc.metadatafile.UpdateCurrentTerm(update.CurrentTerm)
		if err != nil {
			println("todo err")
		}

		err = rc.metadatafile.UpdateVotedFor(update.VotedFor)
		if err != nil {
			println("todo err")
		}
	}

	err = rc.logfile.AppendEntries(output.WriteLogEntries)
	if err != nil {
		println("todo err")
	}

	err = rc.stateMachine.Apply(output.ApplyEntries)
	if err != nil {
		println("todo err")
	}

	for _, msg := range output.SendMessages {
		err = rc.rpc.SendMessage(msg)
		if err != nil {
			println("todo err")
		}
	}

	rc.raft.Advance()
}

func (rc *RaftCoordinator) handleProposal(pr RaftProposal) {

}

func (rc *RaftCoordinator) ProcessMessage(msg raft.RaftMessage) {
	rc.messagec <- msg
}

func (rc *RaftCoordinator) Propose(msg raft.RaftMessage, resp chan error) {
	proposal := RaftProposal{
		Message: msg,
		Error:   resp,
	}

	rc.proposalc <- proposal
}

func (rc *RaftCoordinator) Done() {
	rc.donec <- struct{}{}
}

type testStateMachine struct {
	id     uint64
	values map[string]string
}

func (sm *testStateMachine) Apply(entries []raft.RaftEntry) error {
	for _, e := range entries {
		fmt.Printf("StateMachine #%d: index %d term %d", sm.id, e.Index, e.Term)
	}

	return nil
}
