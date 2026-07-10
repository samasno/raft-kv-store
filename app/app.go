package main

import (
	"net/http"
)

type KVServer struct {
	srv        *http.Server
	kv         KVStore
	raftClient RaftClient
}

func NewKVServer(kv KVStore, rc RaftClient, addr string) *KVServer {
	ks := &KVServer{}

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /record", ks.PutRecord)
	mux.HandleFunc("GET /record", ks.GetRecord)

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

func (k *KVServer) PutRecord(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("put works"))
}

func (k *KVServer) GetRecord(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("get works"))
}

type KVStore interface {
	GetRecord()   // only gets committed entries
	ApplyRecord() // writes committed entries, only in state machine control loop
}
