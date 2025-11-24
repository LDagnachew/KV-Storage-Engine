# Go-Based Storage Engine for Key-Value Pairs

A comprehensive project implementing fundamental storage engine architectures used in modern databases. This project explores how persistent key-value stores work under the hood by building three different storage engine designs from scratch in Go.

## Overview

This project implements three core storage engine architectures:

- **Hash Indexing** - Simple, fast, append-only log with in-memory hash index
- **SSTables + LSM Trees** - Write-optimized Log-Structured Merge Trees
- **B-Trees** - Traditional balanced tree structure with page-based storage

Each implementation demonstrates different trade-offs in performance, complexity, and use cases, providing hands-on experience with the data structures that power systems like PostgreSQL, Cassandra, LevelDB, and RocksDB.

## Project Goals

- Understand the fundamental trade-offs in storage engine design
- Learn how databases achieve persistence and durability
- Explore different indexing strategies and their performance characteristics
- Gain practical experience implementing complex data structures
- Build production-ready storage primitives in Go

## Architecture

### Hash Indexing

The simplest storage engine using an append-only log file with an in-memory hash index.

**Features:**
- O(1) read and write performance
- Append-only log for fast writes
- In-memory hash map for instant lookups
- Background compaction to reclaim space
- Crash recovery from log replay

**Use Cases:** High write throughput, working set fits in memory, simple key-value operations

### SSTables + LSM Trees

A write-optimized structure using sorted string tables and multi-level compaction.

**Features:**
- Memtable (in-memory sorted structure)
- Multiple levels of immutable SSTables on disk
- Background compaction and merging
- Bloom filters for fast negative lookups
- Write-ahead log (WAL) for durability

**Use Cases:** Write-heavy workloads, time-series data, append-mostly patterns

### B-Trees

A traditional balanced tree structure with fixed-size pages stored on disk.

**Features:**
- Self-balancing tree with guaranteed O(log n) operations
- Page-based I/O for efficient disk access
- In-place updates
- Range query support
- Write-ahead logging for crash recovery

**Use Cases:** Read-heavy workloads, range queries, ACID transactions

## Implementation Details

### Hash Indexing

- **Log Format:** Length-prefixed key-value pairs
- **Compaction:** Merges segments and removes duplicates
- **Recovery:** Rebuilds index from log on startup

### LSM Trees

- **Memtable:** Skip list or red-black tree
- **SSTable Format:** Sorted blocks with sparse index
- **Compaction Strategy:** Leveled or size-tiered
- **Optimization:** Bloom filters reduce disk I/O

### B-Trees

- **Node Size:** 4KB pages (configurable)
- **Fanout:** Typically 100-200 children per node
- **Concurrency:** Page-level locking
- **Durability:** WAL with checkpointing

## Target Benchmarks

| Operation | Hash Index | LSM Tree | B-Tree |
|-----------|------------|----------|--------|
| Write     | ~100k/s    | ~80k/s   | ~50k/s |
| Read      | ~150k/s    | ~60k/s   | ~70k/s |
| Range Scan| N/A        | ~40k/s   | ~80k/s |

*Benchmarks run on consumer hardware with default configurations*


## Project Structure

```
.
├── hash/           # Hash indexing implementation
├── lsm/            # LSM Tree implementation
├── btree/          # B-Tree implementation
├── common/         # Shared utilities and interfaces
├── benchmarks/     # Performance tests
└── examples/       # Usage examples
```

## Roadmap

- [x] Hash indexing with compaction
- [ ] LSM Tree with memtable
- [ ] SSTable format and merging
- [ ] B-Tree with page management
- [ ] Write-ahead logging
- [ ] Concurrent access support
- [ ] Snapshot isolation
- [ ] Compression support
- [ ] Bloom filters
- [ ] Performance optimizations

## Learning Resources

- *Designing Data-Intensive Applications* by Martin Kleppmann
- [Database Internals](https://www.databass.dev/) by Alex Petrov
- [LSM Trees Paper](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
- [B-Trees vs LSM Trees](https://tikv.org/deep-dive/key-value-engine/b-tree-vs-lsm/)

## Acknowledgments

Inspired by storage engines in LevelDB, RocksDB, PostgreSQL, and SQLite. Built for learning and understanding database internals.

---

**Note:** This is a personal project. For production use cases, consider mature solutions like BadgerDB, BoltDB, or Pebble.
