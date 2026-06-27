package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/netip"
	"net/url"

	"github.com/samasno/raft-kv/raft"
)

var jsonContentType string = "application/json"
var AppendEntriesPath string = "/append-entries"
var VoteRequestPath string = "/request-vote"

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

func (r *RPC) SendMessage(msg raft.RaftMessage) error {
	return r.sender.SendMessage(msg)
}

type RPCReceiver interface {
	HandleAppendEntries(w http.ResponseWriter, r *http.Request)
	HandleRequestVote(w http.ResponseWriter, r *http.Request)
	Run() error
	Close() error
}

type RPCSender interface {
	SendMessage(raft.RaftMessage) error
}

var _ RPCReceiver = (*RaftServer)(nil)

type RaftServer struct {
	id       uint64
	srv      *http.Server
	receivec chan raft.RaftMessage
}

type RaftServerConfig struct {
	Id    uint64
	Addr  string
	Peers map[uint64]Peer
}

func NewRaftRPC(receivec chan raft.RaftMessage, config RaftServerConfig) (*RPC, error) {
	rs := &RaftServer{
		id:       config.Id,
		receivec: receivec,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST "+AppendEntriesPath, rs.HandleAppendEntries)
	mux.HandleFunc("POST "+VoteRequestPath, rs.HandleRequestVote)

	srv := &http.Server{
		Addr:    config.Addr,
		Handler: mux,
	}

	rs.srv = srv

	rpc := &RPC{
		receiver: rs,
		sender:   nil,
	}

	client := &RaftClient{
		peers: config.Peers,
	}

	rpc.sender = client

	return rpc, nil
}

func (s *RaftServer) HandleAppendEntries(w http.ResponseWriter, r *http.Request) {
	s.handleForwardingRequests(w, r)
}

func (s *RaftServer) HandleRequestVote(w http.ResponseWriter, r *http.Request) {
	s.handleForwardingRequests(w, r)
}

func (s *RaftServer) Run() error {
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			println("listen and serve")
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

func (s *RaftServer) handleForwardingRequests(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError)
		return
	}

	err = s.forwardIncomingRaftMessage(body)
	if err != nil {
		log.Println(err.Error())
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

func writeResponse(w http.ResponseWriter, statusCode int) {
	w.WriteHeader(statusCode)
	fmt.Fprintf(w, "%d %s", statusCode, http.StatusText(statusCode))
}

var _ RPCSender = (*RaftClient)(nil)

type RaftClient struct {
	peers map[uint64]Peer
}

func (c *RaftClient) SendMessage(msg raft.RaftMessage) error {
	peer, ok := c.peers[msg.To]
	if !ok {
		log.Printf("Did not find addressed peer %d\n", msg.To)
		return fmt.Errorf("Peer not found")
	}

	to := peer.Url

	if "" == to {
		to = peer.IP.String()
	}

	to, err := url.JoinPath(peer.Url, getPath(msg.Type))
	if err != nil {
		return err
	}

	body, err := json.Marshal(msg)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	resp, err := http.Post(to, jsonContentType, bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return fmt.Errorf("Msg failed with status %d", resp.StatusCode)
	}

	return nil
}

func getPath(t raft.RaftMessageType) string {
	switch t {
	case raft.MessageAppend, raft.MessageAppendResponse:
		return AppendEntriesPath
	case raft.MessageVoteRequest, raft.MessageVoteResponse, raft.MessagePrevoteRequest, raft.MessagePrevoteResponse:
		return VoteRequestPath
	default:
		return "/"
	}
}

type Peer struct {
	Url string
	IP  netip.Addr
}
