# Distributed Systems in Go

This repository showcases my implementations of distributed systems concepts in Go, based on the renowned MIT 6.5840 / UBC CPSC 416 course. Each lab explores a core concept in distributed systems, including MapReduce, Raft consensus, and sharded key-value stores.

> All code is written in Go and tested on Linux (Ubuntu/WSL).  
> Labs are self-contained and runnable with provided scripts.

---

## 🧐 Overview

| Lab | Topic | Description |
|-----|-------|-------------|
| **Lab 1** | MapReduce | A fault-tolerant MapReduce engine with coordinator-worker architecture via RPC. |
| **Lab 2A-D** | Raft Consensus | A complete Raft implementation, including leader election, log replication, persistence, and snapshot-based log compaction. |
| **Lab 3A-B** | Fault-Tolerant KV Store | A replicated, linearizable key-value store built atop Raft, handling client retries and log snapshotting. |
| **Lab 4A-B** | Sharded KV Store | A dynamic sharded key-value system supporting reconfiguration, data migration, and controller-based shard assignments. |

---

## 🔧 Setup Instructions

### 1. Install Go

Ensure you have **Go 1.17+** installed.  
Install instructions: https://golang.org/doc/install

Check with:
```bash
go version
```

### 2. Clone the Repo

```bash
git clone https://github.com/ritikk7/Distributed-Key-Value-Store.git
cd Distributed-Key-Value-Store
```

### 3. Build Everything

From the project root:

```bash
cd src/main
bash build.sh
```

Or manually:
```bash
cd src/main
go build ../mrapps/wc.go         # Plugin for word count
go build ../mrapps/indexer.go    # Plugin for indexer
go build mrcoordinator.go
go build mrworker.go
go build mrsequential.go
```

---

## 🚀 Running Lab 1: MapReduce

### Word Count Example

```bash
cd src/main

# Clean old outputs
rm -f mr-out* mr-tmp/*

# Run the coordinator (input split files: pg-*.txt)
go run mrcoordinator.go ../pg-*.txt &

# Run workers in parallel
go run mrworker.go ../mrapps/wc.go &
go run mrworker.go ../mrapps/wc.go &
```

When finished, combine the output:

```bash
cat mr-out-* | sort > result.txt
head result.txt
```

### Run All Tests (Lab 1)

```bash
bash test-mr.sh
```

Expected output:
```text
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
...
*** PASSED ALL TESTS
```

---

## 🚀 Running Lab 2: Raft (Parts 2A to 2D)

Lab 2 consists of implementing the [Raft](https://raft.github.io/) consensus algorithm in four parts:

### ✅ 2A - Leader Election
Implements Raft's election protocol using randomized timeouts and heartbeats. Ensures:
- Only one leader at a time
- Leader steps down on term mismatch
- Peers vote once per term

```bash
go test -run 2A
```

### ✅ 2B - Log Replication
Extends Raft to replicate client commands and commit them once stored on a majority of servers.
- Handles mismatched logs
- Applies committed entries via `applyCh`
- Implements `Start()` to initiate replication

```bash
go test -run 2B
```

### ✅ 2C - Persistence
Supports crash recovery by:
- Serializing Raft state via `labgob`
- Saving/restoring state via `Persister`
- Ensuring leaders/peers survive restarts

```bash
go test -run 2C
```

### ✅ 2D - Snapshot & Log Compaction
Supports truncating the log:
- Implements `Snapshot(index, snapshot []byte)`
- Discards old log entries
- Sends/install snapshots to lagging peers
- Persists snapshots and resumes from them on restart

```bash
go test -run 2D
```

You can also run:
```bash
for i in {1..10}; do go test -run 2D; done
```

Each part builds on the last — it's recommended to test each with `-race` enabled:
```bash
go test -race -run 2A
```

---

## 🚀 Running Lab 3: Fault-Tolerant Key/Value Service (3A and 3B)

In Lab 3, a fault-tolerant, linearizable key/value store is built atop your Raft implementation. Clients interact through a Clerk object, and each server uses Raft to ensure consistent replication.

### ✅ 3A - Key/Value Store without Snapshots
Implements:
- Basic Get/Put/Append RPCs
- Coordination via Raft log entries
- Client retries and duplicate request detection

```bash
go test -run 3A
```

Includes tests like:
- One client
- Many clients
- Partitions and failures
- Duplicate RPC handling

### ✅ 3B - Key/Value Store with Snapshots
Adds:
- Log size threshold tracking via `maxraftstate`
- `Snapshot()` integration with Raft
- Log compaction and fast recovery from persisted state

```bash
go test -run 3B
```

Tests include restarts, unreliable networks, partitions, and snapshot correctness.

---

## 🚀 Running Lab 4: Sharded Key/Value Store (4A and 4B)

Lab 4 builds on Lab 3 by supporting **dynamic sharding** across multiple replica groups.

### ✅ 4A - Shard Controller
Implements a centralized controller using Raft that:
- Tracks shard-to-group assignments
- Handles `Join`, `Leave`, `Move`, and `Query` RPCs
- Rebalances shards evenly with minimal movement

```bash
cd src/shardctrler
go test
```

### ✅ 4B - ShardKV: Reconfigurable KV Store
Extends kvraft to dynamically migrate shards between groups:
- Periodically polls the shard controller for config changes
- Transfers shard data between groups via RPC
- Rejects requests for unowned shards with `ErrWrongGroup`
- Ensures linearizability even during reconfiguration

```bash
cd src/shardkv
go test
```

Tests cover:
- Static and dynamic shard assignment
- Concurrent client operations
- Restarts, unreliable networks, partitions
- Correct migration and deletion of shard state

---

## 📚 Lab Writeups

See [`/docs`](https://github.com/ritikk7/Distributed-Key-Value-Store/tree/main/docs) for detailed instructions and explanations for each lab:

- [Lab 1: MapReduce](docs/lab1.md)
- [Lab 2: Raft Consensus](docs/lab2.md)
- [Lab 3: Key/Value Service](docs/lab3.md)
- [Lab 4: Sharded KV Store](docs/lab4.md)

---

## ⚠️ Disclaimer

This project is for educational and demonstration purposes only. It is based on public materials from MIT 6.5840 and UBC CPSC 416, with all implementation written independently.

