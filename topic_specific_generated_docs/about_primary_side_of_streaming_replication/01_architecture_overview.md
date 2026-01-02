# Chapter 1: Synchronous Streaming Replication Architecture

[Index](index.md) | [Next: WAL Generation and LSN Assignment](02_wal_generation_lsn.md) ->

---

## Overview

PostgreSQL's synchronous streaming replication provides a mechanism to ensure that transaction commits are durably replicated to one or more standby servers before the primary acknowledges the commit to the client. This chapter provides a comprehensive architectural overview of the entire system, establishing the foundation for the detailed analysis in subsequent chapters.

The synchronous replication subsystem involves coordination between multiple processes, shared memory structures, and network communication. Understanding the interplay between these components is essential for comprehending the system's behavior and performance characteristics.

**Related Diagrams:**
- [Figure 1: Overall Architecture](diagrams/01_overall_architecture.mermaid) - Complete system component overview
- [Figure 11: Complete Commit Sequence](diagrams/11_complete_commit_sequence.mermaid) - End-to-end commit flow

---

## Processing Flow

The synchronous replication process can be divided into eight distinct phases:

| Phase | Description | Key Functions | Chapter |
|-------|-------------|---------------|---------|
| 1 | WAL Record Insertion | [`XLogInsert()`](#xloginsert), [`XLogInsertRecord()`](#xloginsertrecord) | [Chapter 2](02_wal_generation_lsn.md) |
| 2 | WAL Persistence | [`XLogFlush()`](#xlogflush), [`XLogWrite()`](#xlogwrite) | [Chapter 3](03_wal_persistence.md) |
| 3 | Walsender Transmission | [`WalSndLoop()`](#walsndloop), [`XLogSendPhysical()`](#xlogsendphysical) | [Chapter 4](04_walsender_transmission.md) |
| 4 | Standby Receipt | Walreceiver writes and flushes WAL | - |
| 5 | Standby Reply | Position confirmation sent | [Chapter 6](06_standby_response.md) |
| 6 | Reply Processing | [`ProcessStandbyReplyMessage()`](#processstandbyreplymessage) | [Chapter 6](06_standby_response.md) |
| 7 | Waiter Release | [`SyncRepReleaseWaiters()`](#syncrepreleasewaiters) | [Chapter 7](07_sync_wait_release.md) |
| 8 | Client Response | Commit completion | [Chapter 8](08_client_response.md) |

---

## System Components

### Backend Processes

Backend processes execute SQL transactions and are responsible for:

- **Generating WAL records** via [`XLogInsert()`](02_wal_generation_lsn.md#xloginsert-function) / [`XLogInsertRecord()`](02_wal_generation_lsn.md#xloginsertrecord-function)
- **Flushing WAL to disk** via [`XLogFlush()`](03_wal_persistence.md#xlogflush-function)
- **Waiting for synchronous replication confirmation** via [`SyncRepWaitForLSN()`](07_sync_wait_release.md#syncrepwaitforlsn-function)

### Walsender Processes

Each standby connection spawns a dedicated walsender process that:

- **Streams WAL data** via [`XLogSendPhysical()`](04_walsender_transmission.md#xlogsendphysical-function) or `XLogSendLogical()`
- **Processes standby replies** via [`ProcessStandbyReplyMessage()`](06_standby_response.md#processstandbyreplymessage-function)
- **Releases waiting backends** via [`SyncRepReleaseWaiters()`](07_sync_wait_release.md#syncrepreleasewaiters-function)
- **Manages the replication protocol** in [`WalSndLoop()`](04_walsender_transmission.md#walsndloop-function)

### Walwriter Process

The background walwriter process:

- Performs background WAL flushing via `XLogBackgroundFlush()`
- Handles asynchronous commit persistence
- Reduces flush latency for synchronous commits

### Standby Processes

On the standby server:

- **Walreceiver**: Receives WAL stream and writes to disk
- **Startup Process**: Applies (replays) WAL records

---

## Shared Memory Structures

### XLogCtlData

**Location:** `src/backend/access/transam/xlog.c:451`

The central control structure for WAL management. See [Shared Memory Structures](appendix_glossary.md#xlogctldata) for complete field documentation.

| Field | Type | Description |
|-------|------|-------------|
| `Insert` | XLogCtlInsert | WAL insertion coordination |
| `logInsertResult` | pg_atomic_uint64 | Last byte+1 inserted to buffers |
| `logWriteResult` | pg_atomic_uint64 | Last byte+1 written to disk |
| `logFlushResult` | pg_atomic_uint64 | Last byte+1 flushed (durable) |
| `pages` | char* | WAL buffer pages |
| `xlblocks` | pg_atomic_uint64* | End position per buffer slot |

**Ordering invariant:** `logFlushResult <= logWriteResult <= logInsertResult`

### XLogCtlInsert

**Location:** `src/backend/access/transam/xlog.c:397`

Embedded in XLogCtlData, manages insertion state:

| Field | Type | Description |
|-------|------|-------------|
| `insertpos_lck` | slock_t | Spinlock for byte positions |
| `CurrBytePos` | uint64 | Current reserved position |
| `PrevBytePos` | uint64 | Previous record start |
| `RedoRecPtr` | XLogRecPtr | Current redo point |
| `fullPageWrites` | bool | FPW enabled flag |
| `WALInsertLocks` | WALInsertLockPadded* | Array of insertion locks |

**Cross-reference:** See [Chapter 2](02_wal_generation_lsn.md#reservexloginsertlocation-function) for how these fields are used during LSN assignment.

### WalSndCtlData

**Location:** `src/include/replication/walsender_private.h:91`

Control structure for walsender processes:

| Field | Type | Description |
|-------|------|-------------|
| `SyncRepQueue[3]` | dlist_head | Wait queues (WRITE/FLUSH/APPLY) |
| `lsn[3]` | XLogRecPtr | Confirmed LSN per mode |
| `sync_standbys_status` | bits8 | Sync standby configuration flags |
| `wal_flush_cv` | ConditionVariable | Physical walsender wakeup |
| `walsnds[]` | WalSnd | Per-walsender state array |

**Cross-reference:** See [Chapter 7](07_sync_wait_release.md#syncrepqueue-management) for queue operations.

### WalSnd

**Location:** `src/include/replication/walsender_private.h:42`

Per-walsender state structure:

| Field | Type | Description |
|-------|------|-------------|
| `pid` | pid_t | Walsender PID (0 if inactive) |
| `state` | WalSndState | Current state |
| `sentPtr` | XLogRecPtr | Last sent position |
| `write` | XLogRecPtr | Standby write position |
| `flush` | XLogRecPtr | Standby flush position |
| `apply` | XLogRecPtr | Standby apply position |
| `sync_standby_priority` | int | Priority (0 = async) |
| `latch` | Latch* | For waking this walsender |

**Cross-reference:** See [Figure 5: Walsender State Machine](diagrams/05_walsender_state.mermaid) for state transitions.

---

## Lock Hierarchy

Locks must be acquired in the following order to prevent deadlocks:

| Order | Lock | Purpose | Contention |
|-------|------|---------|------------|
| 1 | WALBufMappingLock | Buffer allocation | Low |
| 2 | WALWriteLock | XLogWrite operations | Medium-High |
| 3 | WALInsertLocks[] | WAL insertion (one of 8) | Medium |
| 4 | insertpos_lck | Position reservation (spinlock) | Very High |
| 5 | SyncRepLock | Queue operations | Medium |
| 6 | info_lck | XLogCtl shared variables (spinlock) | Medium |
| 7 | WalSnd.mutex | Per-walsender state (spinlock) | Low |

**Key optimization:** The `insertpos_lck` spinlock hold time is minimized by using byte position abstraction, reducing contention during high-throughput workloads. See [Chapter 2](02_wal_generation_lsn.md#key-design-insights) for details.

---

## Diagrams

### Figure 1: Overall Architecture

**Location:** [diagrams/01_overall_architecture.mermaid](diagrams/01_overall_architecture.mermaid)

This diagram shows:
- Primary server components (backend, walwriter, walsender)
- Shared memory structures and their relationships
- Standby server components (walreceiver, startup)
- Data and control flow between components

### Figure 11: Complete Commit Sequence

**Location:** [diagrams/11_complete_commit_sequence.mermaid](diagrams/11_complete_commit_sequence.mermaid)

This diagram illustrates the complete end-to-end flow from COMMIT to client response, showing all eight phases of synchronous replication.

---

## Configuration Parameters

| Parameter | Default | Impact | See Also |
|-----------|---------|--------|----------|
| `synchronous_commit` | `on` | Durability level (off/local/remote_write/on/remote_apply) | [Appendix C](appendix_config_params.md#synchronous_commit) |
| `synchronous_standby_names` | `''` | List of sync standby names | [Appendix C](appendix_config_params.md#synchronous_standby_names) |
| `wal_sender_timeout` | `60s` | Time before walsender terminates unresponsive standby | [Chapter 5](05_keepalive_monitoring.md) |
| `wal_receiver_timeout` | `60s` | Time before walreceiver terminates unresponsive primary | - |
| `max_wal_senders` | `10` | Maximum concurrent walsender processes | - |
| `wal_buffers` | `-1` | WAL buffer size (auto-tuned) | [Chapter 3](03_wal_persistence.md) |
| `commit_delay` | `0` | Delay before flush for group commit | [Chapter 3](03_wal_persistence.md#group-commit) |
| `commit_siblings` | `5` | Minimum active transactions for commit_delay | [Chapter 3](03_wal_persistence.md#group-commit) |

---

## Key Takeaways

1. **Multi-process coordination**: Synchronous replication involves coordination across multiple processes (backends, walsenders, walwriter) and shared memory structures.

2. **Sophisticated lock hierarchy**: The WAL subsystem uses a carefully designed lock hierarchy to maximize concurrency while preventing deadlocks.

3. **Atomic progress tracking**: Atomic variables (`logInsertResult`, `logWriteResult`, `logFlushResult`) track WAL progress without heavy locking, enabling lock-free monitoring.

4. **Three synchronization levels**: PostgreSQL offers three synchronization levels for synchronous replication:
   - `remote_write` - Wait for standby write confirmation
   - `on` (remote_flush) - Wait for standby flush confirmation
   - `remote_apply` - Wait for standby replay confirmation

5. **Group commit optimization**: `LWLockAcquireOrWait()` pattern reduces fsync overhead by allowing multiple transactions to share a single fsync operation under high load.

6. **Efficient walsender wakeup**: Condition variables efficiently wake walsenders when new WAL is available, eliminating polling overhead.

7. **LSN-ordered queues**: The SyncRepQueue maintains LSN-ordered waiting backends for efficient batch release when standbys confirm positions.

---

## Navigation

| [Index](index.md) | [Next: WAL Generation and LSN Assignment](02_wal_generation_lsn.md) ->
