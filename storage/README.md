# storage

Persistent storage for the Raft log and node metadata. Implements the `RaftLogFile` and `RaftMetadataFile` interfaces consumed by the Raft core.

## Architecture

**LogFile** stores committed log entries in a binary append-only format across two files:

- `log.bin` — entry payloads with a fixed header (index, term, length)
- `index.bin` — fixed-size index records (index, term, offset, length) for O(1) random access by index

This separation means seeking to an arbitrary entry doesn't require scanning the payload file.

**MetadataFile** stores the two pieces of durable Raft state that must survive crashes:

- `votedFor` — the candidate this node voted for in the current term
- `currentTerm` — the highest term this node has seen

These are written with a magic prefix for basic file validation and synced to disk on every update.

## Usage

```go
logfile, _      := storage.OpenLogFile(dir)
metadatafile, _ := storage.OpenMetadataFile(dir)

// implements raft.RaftLogFile
logfile.AppendEntries(entries)
logfile.GetEntries(start, end)
logfile.GetEntry(index)
logfile.StartOfTerm(term)

// implements raft.RaftMetadataFile
metadatafile.UpdateCurrentTerm(term)
metadatafile.UpdateVotedFor(id)
```

Both files call `Sync()` after each write to flush to disk before returning.

## Limitations

- No log compaction — the log file grows indefinitely; old entries are never reclaimed
- No checksums on log entries — a partially written entry due to a mid-write crash is not detected
- Single directory layout — one node's storage per directory, no multiplexing
