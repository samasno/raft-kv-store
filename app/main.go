package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

// bbolt storage
// http server with put/get
// get should only return committed entries
// put should return an error if not the leader
// want to have immediate response on commit, need to send directly to leader

// kvserver
// put
// propose new record
// check if leader
// if err returns leader
// send with client to leader

// get
// take json encoded get requests
// these should only be commited files

// kv store
// has method to apply committed entries in output.applyentries

// coordinator loop
// accept proposals, messages, ticks, done
// proposal needs to handle non-leader redirect

func main() {
	kv := NewKVServer(nil, nil, "0.0.0.0:8080")

	srvClosed := kv.Run()

	k := make(chan os.Signal, 1)
	signal.Notify(k, syscall.SIGTERM, syscall.SIGINT)

	<-k

	kv.srv.Close()
	err := <-srvClosed
	if err != nil {
		log.Println(err.Error())
	}
}
