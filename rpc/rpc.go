package rpc

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/netip"

	"github.com/samasno/raft-kv/raft"
)

type RPC struct {
	sender   RPCSender
	receiver RPCReceiver
}

func (r *RPC) Run() error {
	return r.receiver.Run()
}

func (r *RPC) Close() error {
	return r.receiver.Close()
}

type RPCReceiver interface {
	HandleAppendEntries(w http.ResponseWriter, r *http.Request)
	HandleRequestVote(w http.ResponseWriter, r *http.Request)
	Run() error
	Close() error
}

type RPCSender interface {
	SendMessages([]raft.RaftMessage, error)
}

type RaftServer struct {
	id       uint64
	srv      *http.Server
	receivec chan raft.RaftMessage
}

type RaftServerConfig struct {
	Id    uint64
	Peers map[uint64]Peer
}

func NewRaftRPC(receivec chan raft.RaftMessage, config RaftServerConfig) (*RPC, error) {
	rs := &RaftServer{
		id:       config.Id,
		receivec: receivec,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /append-entries", rs.HandleAppendEntries)
	mux.HandleFunc("POST /request-vote", rs.HandleRequestVote)

	srv := &http.Server{
		Addr:    "0.0.0.0:8080",
		Handler: mux,
	}

	rs.srv = srv

	rpc := &RPC{
		receiver: rs,
		sender:   nil,
	}

	return rpc, nil
}

func (s *RaftServer) HandleAppendEntries(w http.ResponseWriter, r *http.Request) {
	s.handleForwardingRequests(w, r)
}

func (s *RaftServer) HandleRequestVote(w http.ResponseWriter, r *http.Request) {
	s.handleForwardingRequests(w, r)
}

func (s *RaftServer) handleForwardingRequests(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError)
		return
	}

	err = s.forwardIncomingRaftMessage(body)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError)
		return
	}

	writeResponse(w, http.StatusOK)
}

func (s *RaftServer) forwardIncomingRaftMessage(data []byte) error {
	msg := raft.RaftMessage{}
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return err
	}

	s.receivec <- msg
	return nil
}

func (s *RaftServer) Run() error {
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Println(err.Error())
		}
	}()
	return nil
}

func (s *RaftServer) Close() error {
	err := s.srv.Close()
	if err != nil {
		return err
	}

	return nil
}

type RaftClient struct {
	peers map[uint64]Peer
}

type Peer struct {
	Hostname string
	IP       netip.Addr
}

func writeResponse(w http.ResponseWriter, statusCode int) {
	w.WriteHeader(statusCode)
	fmt.Fprintf(w, "%d %s", statusCode, http.StatusText(statusCode))
}
