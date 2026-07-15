# raft-noob

An educational implementation of the Raft consensus algorithm in Go. This is not production software — it's a learning project built to understand how distributed consensus works in practice.

## What it implements

- **Leader election** with pre-vote phase to avoid disruptive elections
- **Log replication** from leader to followers with catch-up for lagging nodes
- **Crash recovery** via persistent log and metadata files replayed on restart
- **Key-value store** layered on top of the Raft core, exposing a simple HTTP API

## Project layout

```
raft/       Core Raft state machine (election, replication, commit logic)
app/        KV server and coordinator wiring Raft to the HTTP API
rpc/        HTTP-based transport for Raft messages between nodes
storage/    Append-only log file and metadata file for durability
```

## Running a 3-node cluster

Requires Docker and Docker Compose.

```bash
docker compose up --build
```

This starts three nodes. The KV HTTP API is available on ports 9001, 9002, and 9003.

## API

All operations go through a single endpoint. Reads are served locally by any node; writes are routed to the current leader automatically.

**Set a key**
```bash
curl -X POST http://localhost:9001/record \
  -d '{"op":"SET","key":"foo","value":"bar"}'
```

**Get a key**
```bash
curl -X POST http://localhost:9001/record \
  -d '{"op":"GET","key":"foo"}'
```

**Delete a key**
```bash
curl -X POST http://localhost:9001/record \
  -d '{"op":"DEL","key":"foo"}'
```

## Running tests

```bash
go test ./...
```

The integration test (`TestKVServerCluster`) spins up a 3-node in-process cluster and exercises leader election, replication, and follower reads. It takes about 15 seconds to allow election to stabilize.

## How it works

Each node runs a Raft state machine driven by a tick-based control loop. The node starts as a follower. If it doesn't hear from a leader within its election timeout, it kicks off a pre-vote round to check whether a quorum is reachable before incrementing its term. If pre-vote succeeds, it campaigns for leadership. Once elected, the leader writes a no-op entry to its log to establish its term, then begins accepting client writes.

Client writes are proposed to the leader's Raft instance. The leader replicates the entry to followers and commits once a quorum acknowledges it. The commit unblocks the waiting HTTP handler, which returns a response to the client.

On restart, each node replays its persisted log into the state machine up to the last checkpointed index.

## Limitations

This is a simplified implementation. It does not handle membership changes, log compaction/snapshotting, or linearizable reads.
