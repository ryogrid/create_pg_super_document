# Executive Summary: PostgreSQL MVCC

> MVCC Documentation > Executive Summary

---

## What Is MVCC?

Multi-Version Concurrency Control (MVCC) is PostgreSQL's core mechanism for enabling concurrent data access without read locks. Instead of allowing only one transaction to read or write data at a time, MVCC maintains multiple physical versions of each row so that readers and writers can operate simultaneously on the same data without blocking each other.

When a transaction modifies a row, PostgreSQL does not overwrite the existing data. Instead, it creates a new version of the row (for UPDATE) or marks the existing version as deleted (for DELETE), stamping each version with the transaction ID (XID) of the modifying transaction. Each transaction sees a consistent snapshot of the database as of the moment its query (or transaction) began, regardless of concurrent modifications by other transactions.

## Why PostgreSQL Uses MVCC

PostgreSQL chose MVCC over lock-based concurrency for several key reasons:

- **Readers never block writers, and writers never block readers.** A SELECT never waits for an UPDATE on the same row, and vice versa. Only two concurrent writers targeting the same row will conflict.
- **No read locks are needed.** Traditional lock-based systems require shared locks on every row read, which creates significant contention under read-heavy workloads. MVCC eliminates this overhead entirely.
- **Consistent snapshots without serialization.** Each transaction sees a self-consistent view of the database without requiring the system to serialize all operations.
- **Rollback is instantaneous.** Since old versions are not overwritten, aborting a transaction simply means discarding the new versions -- no undo log replay is needed.

## Core Components at a Glance

PostgreSQL's MVCC implementation comprises seven interrelated subsystems:

| Subsystem | Responsibility | Primary Source Files |
|-----------|---------------|---------------------|
| **Transaction Lifecycle** | Managing transaction states, XID allocation, commit/abort processing | `xact.c`, `varsup.c` |
| **Tuple Versioning** | Creating, linking, and managing multiple physical row versions | `heapam.c`, `htup_details.h` |
| **Visibility Rules** | Determining which tuple version is visible to each transaction | `heapam_visibility.c` |
| **Snapshot Management** | Capturing and maintaining point-in-time views of transaction state | `procarray.c`, `snapmgr.c` |
| **Concurrency Infrastructure** | ProcArray, PGPROC shared memory, SSI predicate locking | `proc.h`, `procarray.c`, `predicate.c` |
| **CLOG (Commit Log)** | Persistent two-bit-per-transaction commit/abort status store | `clog.c`, `transam.c` |
| **VACUUM** | Garbage collecting dead tuple versions and freezing old XIDs | `vacuumlazy.c`, `pruneheap.c` |

## Design Philosophy

PostgreSQL's MVCC design follows several guiding principles:

1. **Optimistic concurrency.** The system assumes conflicts are rare and detects them at write time rather than preventing them with upfront locking.

2. **Lazy evaluation.** Transaction IDs are not assigned until the first write operation, keeping read-only transactions lightweight. Hint bits cache CLOG lookups lazily on first access.

3. **Tuple-level versioning.** Each row version is a self-contained physical copy with embedded MVCC metadata (xmin, xmax, infomask flags). There is no separate undo log.

4. **Deferred cleanup.** Dead tuple versions are not removed at transaction commit time. Instead, VACUUM runs as a separate background process to reclaim space, decoupling transaction throughput from garbage collection overhead.

## Key Trade-offs

MVCC is not without costs. Understanding these trade-offs is essential for operating PostgreSQL effectively:

| Benefit | Cost |
|---------|------|
| No read locks | UPDATE creates a full physical copy of the row, consuming additional storage |
| Instant rollback | Dead row versions accumulate until VACUUM removes them (table bloat) |
| Consistent snapshots | The snapshot mechanism requires scanning the ProcArray shared structure |
| Simple crash recovery | 32-bit transaction IDs require periodic freezing to prevent wraparound |
| Writer isolation | VACUUM must run regularly; failure to vacuum can lead to transaction ID wraparound shutdown |

The most operationally significant consequence is the **VACUUM requirement**: because dead tuple versions are not removed inline, a background process must periodically scan tables, identify dead versions, and reclaim their storage. If VACUUM falls behind, tables grow larger than necessary (bloat), and in extreme cases, the system will refuse new write transactions to prevent XID wraparound.

## Reading This Documentation

This documentation is organized from high-level concepts to implementation details:

- **New to MVCC?** Start with the [Architecture Overview](02_architecture_overview.md) for the big picture, then read [Visibility Rules](05_visibility_rules.md) to understand the core algorithm.
- **Investigating a performance issue?** See [Snapshot Management](06_snapshot_management.md) for ProcArray contention and [VACUUM and Freezing](09_vacuum_and_freezing.md) for bloat-related issues.
- **Contributing to PostgreSQL?** Read the chapters in order (03-09) for complete coverage, then consult the [Deep Dives](10_deep_dives.md) for advanced topics like SSI and HOT chains.
- **Quick lookup?** Use the [Symbol Index](appendix_symbol_index.md), [API Reference](mvcc_api_reference.md), or [Quick Reference Card](mvcc_quick_reference.md).

## Document Statistics

| Metric | Value |
|--------|-------|
| Total symbols documented | 74 (30 in depth, 44 with overview) |
| Key functions with full walkthroughs | 30 |
| Mermaid diagrams | 8 |
| Source files covered | 17 |
| PostgreSQL version | 17.6 |

---

Next: [Architecture Overview](02_architecture_overview.md)
