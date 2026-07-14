package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/samasno/raft-kv/raft"
)

const baseRecordPath = "/record"

type KVServer struct {
	srv        *http.Server
	kv         KVStore
	raftClient RaftClient
	peers      map[uint64]string
}

func NewKVServer(kv KVStore, rc RaftClient, addr string, peers map[uint64]string) *KVServer {
	ks := &KVServer{
		kv:         kv,
		raftClient: rc,
		peers:      peers,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST "+baseRecordPath, ks.HandleRequest)

	ks.srv = &http.Server{
		Handler: mux,
		Addr:    addr,
	}

	return ks
}

func (k *KVServer) Run() chan error {
	done := make(chan error)
	go func(c chan error) {
		if err := k.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			c <- err
		}

		c <- nil
	}(done)

	return done
}

func (k *KVServer) Close() {
	k.srv.Close()
}

func readCommand(r io.Reader) (Command, error) {
	command := Command{}
	commandBytes, err := io.ReadAll(r)
	if err != nil {
		return command, err
	}

	err = json.Unmarshal(commandBytes, &command)
	if err != nil {
		return command, err
	}

	return command, nil
}

func (k *KVServer) HandleRequest(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	command, err := readCommand(r.Body)
	if err != nil {
		serverError(w)
		return
	}

	response, err := k.handleCommand(command)
	if err != nil {
		serverError(w)
		return
	}

	if !response.Success {
		writeResponse(w, response, http.StatusBadRequest)
		return
	}

	writeResponse(w, response, http.StatusOK)
}

func (k *KVServer) handleCommand(command Command) (Response, error) {
	op := KVOps(strings.ToUpper(string(command.Op)))
	switch op {
	case GET:
		return k.handleGet(command)
	case DEL, SET:
		return k.handleProposal(command)
	default:
		return Response{Success: false, Key: "", Value: "Invalid op"}, nil
	}
}

func (k *KVServer) handleProposal(command Command) (Response, error) {
	response := Response{Key: command.Key}
	commandJson, err := json.Marshal(command)
	if err != nil {
		return response, err
	}

	msg := raft.RaftMessage{
		Type:       raft.MessageNewEntry,
		RawEntries: [][]byte{commandJson},
	}

	responsec := make(chan RaftProposalResponse)
	k.raftClient.Propose(msg, responsec)
	raftResponse := <-responsec

	if !raftResponse.Success {
		return k.sendCommandToPeer(raftResponse.LeaderId, command)
	}

	response.Success = true
	return response, nil
}

func (k *KVServer) sendCommandToPeer(peerId uint64, command Command) (Response, error) {
	peerResponse := Response{}

	peerUrl, ok := k.peers[peerId]
	if !ok {
		return peerResponse, fmt.Errorf("No peer with this id found")
	}

	commandJson, err := json.Marshal(command)
	if err != nil {
		return peerResponse, err
	}

	to, err := url.JoinPath(peerUrl, baseRecordPath)
	if err != nil {
		return peerResponse, err
	}

	req, err := http.NewRequest(http.MethodPost, to, bytes.NewBuffer(commandJson))
	if err != nil {
		return peerResponse, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return peerResponse, err
	}

	resB, err := io.ReadAll(res.Body)
	if err != nil {
		return peerResponse, err
	}

	err = json.Unmarshal(resB, &peerResponse)
	if err != nil {
		return peerResponse, err
	}

	return peerResponse, nil
}

func (k *KVServer) handleGet(command Command) (Response, error) {
	value := k.kv.Get(command.Key)
	response := Response{
		Success: true,
		Key:     command.Key,
		Value:   value,
	}

	return response, nil
}

func writeResponse(w http.ResponseWriter, response Response, status int) {
	responseJson, err := json.Marshal(response)
	if err != nil {
		serverError(w)
		return
	}

	w.WriteHeader(status)
	w.Write(responseJson)
}

func serverError(w http.ResponseWriter) {
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte("Server Error"))
}

type KVStore interface {
	Get(string) string                  // only gets committed entries
	ApplyRecord([]raft.RaftEntry) error // writes committed entries, only in state machine control loop
}
