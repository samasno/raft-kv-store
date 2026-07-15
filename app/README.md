# app

Wires the Raft core to a persistent key-value store exposed over HTTP. Two main components: the coordinator and the KV server.

## Architecture

**Coordinator** drives the Raft control loop. It multiplexes inbound RPC messages, client proposals, and ticks onto the Raft state machine, then handles each output — persisting log entries, updating metadata, sending outbound RPC messages, and applying committed entries to the state machine. Proposals block until the entry is committed.

**KVServer** is the HTTP layer. It accepts all operations through a single `POST /record` endpoint and dispatches by the `op` field in the request body. GET is served from local state directly. SET and DEL are proposed through the coordinator. If the receiving node is not the leader, it proxies the request to the leader's KV address.

```
POST /record  {"op":"SET","key":"foo","value":"bar"}
POST /record  {"op":"GET","key":"foo"}
POST /record  {"op":"DEL","key":"foo"}
```

**KVMap** is the state machine. It holds the in-memory key-value map, applies committed entries, and maintains a checkpoint file so restarts can replay the log from where they left off rather than from the beginning.

## Usage

```go
coord := NewCoordinator()
logfile, _ := coord.OpenStorage(storageDir)

kv, _ := NewKvMap(checkpointPath, logfile)

server := NewKVServer(kv, coord, addr, peers)
server.Run()

coord.StartControlLoop(raftConfig, rpcConfig, storageDir, kv)
```

`peers` is a `map[uint64]string` of node ID to KV HTTP base URL, used for leader proxying.

## Limitations

- GET is served from local state without going through Raft — reads are not linearizable. A follower may return stale data, and a partitioned leader has no lease check. Fix requires routing GET through the leader with a heartbeat-based lease.
- No client deduplication — a retried write after a leader crash can be applied twice.
