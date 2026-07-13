package main

import (
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/samasno/raft-kv/raft"
	"github.com/samasno/raft-kv/rpc"
)

func main() {
	nodeID := mustEnvUint64("NODE_ID")
	raftAddr := mustEnv("RAFT_ADDR")
	kvAddr := mustEnv("KV_ADDR")
	storageDir := mustEnv("STORAGE_DIR")

	// PEERS and KV_PEERS use the format: id=url,id=url
	// e.g. PEERS=2=http://node2:8000,3=http://node3:8000
	raftPeerURLs := parsePeerMap(mustEnv("PEERS"))
	kvPeerURLs := parsePeerMap(mustEnv("KV_PEERS"))

	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("create storage dir: %v", err)
	}

	peerIDs := make([]uint64, 0, len(raftPeerURLs))
	rpcPeers := map[uint64]rpc.Peer{}
	for id, url := range raftPeerURLs {
		peerIDs = append(peerIDs, id)
		rpcPeers[id] = rpc.Peer{Url: url}
	}

	raftConfig := &raft.RaftConfig{
		Id:    nodeID,
		Peers: peerIDs,
	}

	rpcConfig := &rpc.RaftServerConfig{
		Id:    nodeID,
		Addr:  raftAddr,
		Peers: rpcPeers,
	}

	coord := NewCoordinator()

	// OpenStorage before NewKvMap so the logfile can be wired as the LogReader,
	// enabling replay of committed entries on restart.
	logFile, err := coord.OpenStorage(storageDir)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}

	kv, err := NewKvMap(filepath.Join(storageDir, "checkpoint"), logFile)
	if err != nil {
		log.Fatalf("NewKvMap: %v", err)
	}

	server := NewKVServer(kv, coord, kvAddr, kvPeerURLs)

	errc := make(chan error, 2)

	go func() {
		errc <- coord.StartControlLoop(raftConfig, rpcConfig, storageDir, kv)
	}()

	serverDone := server.Run()
	go func() {
		if err := <-serverDone; err != nil {
			errc <- err
		}
	}()

	log.Printf("node %d ready — raft=%s kv=%s", nodeID, raftAddr, kvAddr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	select {
	case <-sig:
		log.Println("shutting down")
		server.Close()
		coord.Done()
	case err := <-errc:
		log.Fatalf("fatal: %v", err)
	}
}

// parsePeerMap parses "id=url,id=url" into a map[uint64]string.
func parsePeerMap(s string) map[uint64]string {
	peers := map[uint64]string{}
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("invalid peer entry %q — want id=url", entry)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil {
			log.Fatalf("invalid peer id %q: %v", parts[0], err)
		}
		peers[id] = strings.TrimSpace(parts[1])
	}
	return peers
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env var %s is not set", key)
	}
	return v
}

func mustEnvUint64(key string) uint64 {
	v := mustEnv(key)
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		log.Fatalf("env var %s must be a positive integer, got %q", key, v)
	}
	return n
}
