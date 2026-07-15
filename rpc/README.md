# rpc

HTTP-based transport for Raft messages between nodes. Handles sending outbound messages to peers and receiving inbound messages from them, then forwarding to the Raft state machine via a channel.

## Architecture

Two sub-components:

**Sender** makes outgoing HTTP POST requests to peer nodes. Each Raft message type is routed to its corresponding path (`/append-entries` or `/request-vote`). Messages are JSON-encoded.

**Receiver** runs an HTTP server that accepts inbound messages on those same paths, decodes them, and pushes them onto a shared message channel that the coordinator's control loop reads from.

```
Outbound:  RaftMessage → JSON → HTTP POST → peer
Inbound:   HTTP POST → JSON → RaftMessage → messagec
```

## Usage

```go
config := &rpc.RaftServerConfig{
    Id:   nodeId,
    Addr: "0.0.0.0:8000",
    Peers: map[uint64]rpc.Peer{
        2: {Url: "http://node2:8000"},
        3: {Url: "http://node3:8000"},
    },
}

r, _ := rpc.NewRaftRPC(messagec, *config)
r.Run()
r.SendMessage(msg)
r.Close()
```

## Limitations

- HTTP is higher overhead than a binary protocol (gRPC, raw TCP) — acceptable for a demo cluster but not suitable for high-throughput production use
- No TLS, no authentication — assumes a trusted network
- No retry logic on send failures — dropped messages rely on Raft's timeout and retry mechanisms
