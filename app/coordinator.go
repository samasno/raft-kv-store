package main

import (
	"fmt"
	"sync"
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
	proposalc chan RaftProposalRequest
	messagec  chan raft.RaftMessage
	donec     chan struct{}

	mtx *sync.Mutex

	commitIndex   uint64
	commitIndexWC sync.Cond

	// output handlers
	logfile      *storage.LogFile
	metadatafile *storage.MetadataFile
	rpc          *rpc.RPC
	raft         *raft.Raft

	stateMachine StateMachine
}

func NewCoordinator() *RaftCoordinator {
	return &RaftCoordinator{
		proposalc:     make(chan RaftProposalRequest),
		messagec:      make(chan raft.RaftMessage),
		donec:         make(chan struct{}),
		commitIndexWC: *sync.NewCond(&sync.Mutex{}),
	}
}

type RaftProposalRequest struct {
	Message  raft.RaftMessage
	Response chan RaftProposalResponse
}

type RaftProposalResponse struct {
	Success  bool
	LeaderId uint64
}

func (rc *RaftCoordinator) StartControlLoop(raftConfig *raft.RaftConfig, rpcConfig *rpc.RaftServerConfig, storageDir string, stateMachine StateMachine) error {
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

	rc.stateMachine = stateMachine

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
			rc.handleTick()
		}
		if exit {
			break
		}
	}

	return nil
}

func (rc *RaftCoordinator) handleMessage(msg raft.RaftMessage) {
	rc.raft.Call(msg)
	output := <-rc.raft.Ready()

	err := rc.handleOutput(output)
	if err != nil {
		println("todo err")
	}

	rc.raft.Advance()
}

func (rc *RaftCoordinator) handleTick() {
	rc.raft.Tick()
	output := <-rc.raft.Ready()

	err := rc.handleOutput(output)
	if err != nil {
		println("todo err")
	}

	rc.raft.Advance()
}

func (rc *RaftCoordinator) handleOutput(output *raft.RaftOutput) error {
	var err error
	for _, update := range output.UpdateMetadata {
		err = rc.metadatafile.UpdateCurrentTerm(update.CurrentTerm)
		if err != nil {
			return err
		}

		err = rc.metadatafile.UpdateVotedFor(update.VotedFor)
		if err != nil {
			return err
		}
	}

	err = rc.logfile.AppendEntries(output.WriteLogEntries)
	if err != nil {
		return err
	}

	err = rc.stateMachine.Apply(output.ApplyEntries)
	if err != nil {
		println("todo err")
	}

	if 0 < len(output.ApplyEntries) {
		rc.commitIndexWC.L.Lock()
		rc.commitIndex = output.ApplyEntries[len(output.ApplyEntries)-1].Index
		rc.commitIndexWC.L.Unlock()
		rc.commitIndexWC.Broadcast()
	}

	for _, msg := range output.SendMessages {
		err = rc.rpc.SendMessage(msg)
		if err != nil {
			return err
		}
	}

	return nil
}

func (rc *RaftCoordinator) handleProposal(preq RaftProposalRequest) {
	presp := RaftProposalResponse{}

	rc.raft.Call(preq.Message)
	output := <-rc.raft.Ready()
	if 0 < len(output.SendMessages) && output.SendMessages[0].Type == raft.MessageNotLeader {
		msg := output.SendMessages[0]
		presp.Success = false
		presp.LeaderId = msg.LeaderId
		preq.Response <- presp
		return
	}

	// check if any write entries
	var err error
	err = rc.logfile.AppendEntries(output.WriteLogEntries)
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

	lastEntry := output.WriteLogEntries[len(output.WriteLogEntries)-1]

	go func() {
		rc.commitIndexWC.L.Lock()
		defer rc.commitIndexWC.L.Unlock()
		for !(rc.commitIndex >= lastEntry.Index) {
			rc.commitIndexWC.Wait()
		}

		presp.Success = true
		preq.Response <- presp
	}()
}

func (rc *RaftCoordinator) ProcessMessage(msg raft.RaftMessage) {
	rc.messagec <- msg
}

func (rc *RaftCoordinator) Propose(msg raft.RaftMessage, resp chan RaftProposalResponse) {
	proposal := RaftProposalRequest{
		Message:  msg,
		Response: resp,
	}

	rc.proposalc <- proposal
	// if error returned, need to check leader and send there
}

func (rc *RaftCoordinator) Done() {
	rc.donec <- struct{}{}
}

type testStateMachine struct {
	id     uint64
	values map[string]string
}

func newTestStateMachine(id uint64) *testStateMachine {
	return &testStateMachine{
		id:     id,
		values: map[string]string{},
	}
}

func (sm *testStateMachine) Apply(entries []raft.RaftEntry) error {
	for _, e := range entries {
		fmt.Printf("StateMachine #%d: index %d term %d", sm.id, e.Index, e.Term)
		key := fmt.Sprintf("%d", e.Index)
		sm.values[key] = string(e.Payload)
	}

	return nil
}

func (sm *testStateMachine) debugPrintValues() {
	println("***Start StateMachine Values***")
	for k, v := range sm.values {
		fmt.Printf("%s: %s", k, v)
	}
	println("***End StateMachine Values***")
}

func testingRpcConfig(count int) map[uint64]*rpc.RaftServerConfig {
	peers := map[uint64]rpc.Peer{}
	// make peers
	for i := uint64(1); i <= uint64(count); i++ {
		peer := rpc.Peer{
			Url: fmt.Sprintf("127.0.0.1:800%d", i),
		}
		peers[i] = peer
	}

	configs := map[uint64]*rpc.RaftServerConfig{}
	for k, v := range peers {
		config := &rpc.RaftServerConfig{
			Id:    k,
			Addr:  v.Url,
			Peers: peers,
		}

		configs[k] = config
	}
	// make config for each peer
	return configs
}

func testingRaftConfigs(count int) map[uint64]*raft.RaftConfig {
	configs := map[uint64]*raft.RaftConfig{}
	for i := uint64(1); i <= uint64(count); i++ {
		peers := []uint64{}
		for j := uint64(1); j <= uint64(count); j++ {
			if j != i {
				peers = append(peers, j)
			}
		}

		config := &raft.RaftConfig{
			Id:    i,
			Peers: peers,
		}

		configs[i] = config
	}

	return configs
}
