# raft

Core Raft state machine. Handles leader election, log replication, and commit tracking. Has no knowledge of networking or storage — all I/O is handed back to the caller through a structured output type.

## Architecture

The state machine runs in its own goroutine and communicates through channels using a Ready/Advance pattern:

1. Caller sends a message or tick via `Call(msg)` or `Tick()`
2. State machine processes it and publishes output to `Ready()`
3. Caller reads the output, performs I/O (write to disk, send over network, apply to state machine)
4. Caller calls `Advance()` to signal the state machine to update its internal indexes

This keeps the Raft logic pure and testable — the state machine never touches a file or a socket.

## States

- **Follower** — default state, waits for heartbeats from the leader
- **Precandidate** — runs a pre-vote round before incrementing term, avoids disruptive elections
- **Candidate** — campaigns for leadership by requesting votes
- **Leader** — replicates entries, tracks follower progress, advances commit index

On election win the leader immediately writes a no-op entry to establish its term in the log before accepting client writes.

## API

```go
r, _ := raft.NewRaftInstance(metadataFile, logFile, config)

r.Call(msg)       // deliver an inbound message
r.Tick()          // advance the election/heartbeat clock
output := <-r.Ready()  // read what needs to happen next
r.Advance()       // commit the output back to raft's internal state
r.Done()          // shut down
```

`RaftOutput` fields:

| Field | Meaning |
|---|---|
| `WriteLogEntries` | Entries to append to durable log |
| `UpdateMetadata` | Term/votedFor changes to persist |
| `SendMessages` | Messages to deliver to peers |
| `ApplyEntries` | Committed entries ready for the state machine |

## Limitations

- No log compaction or snapshotting — the log grows without bound
- No dynamic membership changes — cluster size is fixed at startup
- Reads are not linearizable — no leader lease or ReadIndex implementation
