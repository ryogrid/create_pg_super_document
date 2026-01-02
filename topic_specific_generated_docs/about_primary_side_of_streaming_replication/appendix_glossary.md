# Appendix B: Glossary

[Index](index.md)

---

This appendix provides definitions for technical terminology used throughout this documentation, with a focus on synchronous replication and WAL concepts.

---

## Core Concepts

### LSN (Log Sequence Number)

A 64-bit value representing a position in the WAL stream. Stored as `XLogRecPtr` type.

**Format:** `{segment}:{offset}` in display (e.g., `0/1234ABC`)

**Components:**
- Upper 32 bits: Segment-level position
- Lower 32 bits: Offset within segment

**Key Operations:**
- Comparison: LSN A < LSN B means A occurred before B
- Arithmetic: LSN B - LSN A gives bytes between positions

**Related:** [Chapter 2: LSN Assignment](02_wal_generation_lsn.md#reservexloginsertlocation-function)

---

### WAL (Write-Ahead Log)

A sequential log of all changes to the database, written before the corresponding data file changes. WAL provides:

1. **Crash recovery**: Replay incomplete changes after crash
2. **Streaming replication**: Transfer changes to standbys
3. **Point-in-time recovery**: Recover to any point in WAL history

**Components:**
- WAL records: Individual change entries
- WAL pages: 8KB blocks (default XLOG_BLCKSZ)
- WAL segments: 16MB files (default wal_segment_size)

**Related:** [Chapter 1: Architecture](01_architecture_overview.md)

---

### XLogRecPtr

The C type for LSN values:

```c
typedef uint64 XLogRecPtr;
```

**Special values:**
- `InvalidXLogRecPtr = 0` - No valid position
- Used for pointers like `xl_prev` linking records

---

### Synchronous Replication

A replication mode where transaction commit waits until one or more standbys confirm receipt of the WAL. Provides strong durability guarantees.

**Modes (via `synchronous_commit`):**
| Level | Wait For | Code Constant |
|-------|----------|---------------|
| `off` | Nothing | - |
| `local` | Local flush | - |
| `remote_write` | Standby write | SYNC_REP_WAIT_WRITE |
| `on` | Standby flush | SYNC_REP_WAIT_FLUSH |
| `remote_apply` | Standby apply | SYNC_REP_WAIT_APPLY |

**Related:** [Chapter 7: Sync Wait/Release](07_sync_wait_release.md)

---

### Streaming Replication

A method of replicating WAL from primary to standby servers in near-real-time via TCP connections. Can be synchronous or asynchronous.

**Components:**
- Primary: walsender processes
- Standby: walreceiver and startup processes

**Related:** [Chapter 4: Walsender Transmission](04_walsender_transmission.md)

---

## Processes

### Backend Process

A PostgreSQL process that handles a client connection, executing SQL statements. In sync rep context:
- Generates WAL via `XLogInsert()`
- Flushes WAL via `XLogFlush()`
- Waits for confirmation via `SyncRepWaitForLSN()`

---

### Walsender Process

A PostgreSQL background process that streams WAL to a standby server. One walsender per standby connection.

**States:**
| State | Description |
|-------|-------------|
| STARTUP | Initial state |
| BACKUP | Performing base backup |
| CATCHUP | Sending historical WAL |
| STREAMING | Caught up, sending in real-time |
| STOPPING | Graceful shutdown |

**Related:** [Chapter 4](04_walsender_transmission.md), [Figure 5](diagrams/05_walsender_state.mermaid)

---

### Walreceiver Process

A PostgreSQL background process on a standby that receives WAL from the primary's walsender. Writes WAL to disk and signals startup process.

---

### Walwriter Process

A PostgreSQL background process that periodically flushes WAL buffers to disk. Reduces commit latency by proactively flushing.

**Related:** [Chapter 1: Architecture](01_architecture_overview.md)

---

### Startup Process

A PostgreSQL background process on a standby that replays (applies) WAL records to bring the database up-to-date.

---

## Shared Memory Structures

### XLogCtlData

Central control structure for WAL management. Contains:
- Insertion coordination (`XLogCtlInsert Insert`)
- Progress tracking (`logInsertResult`, `logWriteResult`, `logFlushResult`)
- WAL buffer pages (`pages`)
- Buffer position tracking (`xlblocks`)

**Location:** `src/backend/access/transam/xlog.c:451`
**Related:** [Chapter 1: Shared Memory](01_architecture_overview.md#xlogctldata)

---

### XLogCtlInsert

Embedded structure in XLogCtlData managing insertion state:
- Position reservation (`CurrBytePos`, `PrevBytePos`)
- Insertion locks (`WALInsertLocks`)
- FPW state (`fullPageWrites`, `RedoRecPtr`)

**Location:** `src/backend/access/transam/xlog.c:397`
**Related:** [Chapter 2: LSN Assignment](02_wal_generation_lsn.md#reservexloginsertlocation-function)

---

### WalSndCtlData

Control structure for walsender coordination:
- Sync rep queues (`SyncRepQueue[3]`)
- Confirmed positions (`lsn[3]`)
- Condition variables (`wal_flush_cv`, `wal_replay_cv`)
- Per-walsender array (`walsnds[]`)

**Location:** `src/include/replication/walsender_private.h:91`
**Related:** [Chapter 1](01_architecture_overview.md#walsndctldata), [Chapter 7](07_sync_wait_release.md)

---

### WalSnd

Per-walsender state structure:
- Process info (`pid`, `state`)
- Send progress (`sentPtr`)
- Standby positions (`write`, `flush`, `apply`)
- Sync rep priority (`sync_standby_priority`)

**Location:** `src/include/replication/walsender_private.h:42`
**Related:** [Chapter 1](01_architecture_overview.md#walsnd)

---

### SyncRepQueue

Array of three doubly-linked lists holding backends waiting for sync rep confirmation:
- `SyncRepQueue[SYNC_REP_WAIT_WRITE]` - remote_write waiters
- `SyncRepQueue[SYNC_REP_WAIT_FLUSH]` - on (remote_flush) waiters
- `SyncRepQueue[SYNC_REP_WAIT_APPLY]` - remote_apply waiters

**Related:** [Chapter 7](07_sync_wait_release.md), [Figure 10](diagrams/10_syncrep_queue_state.mermaid)

---

## Operations and Concepts

### Byte Position

An internal abstraction used during LSN assignment. Counts only "usable" bytes (excludes page headers), allowing simple arithmetic under spinlock.

**Conversion functions:**
- `XLogBytePosToRecPtr()` - byte position to start LSN
- `XLogBytePosToEndRecPtr()` - byte position to end LSN
- `XLogRecPtrToBytePos()` - LSN to byte position

**Related:** [Chapter 2](02_wal_generation_lsn.md#key-design-insights)

---

### Group Commit

Optimization where multiple concurrent transactions share a single WAL flush (fsync) operation. Implemented via `LWLockAcquireOrWait()`:

1. First backend acquires WALWriteLock, others wait
2. First backend flushes, releases lock
3. Waiters check if their LSN is now flushed
4. If flushed, return without doing any I/O

**Related:** [Chapter 3](03_wal_persistence.md#lwlockacquireorwait---group-commit-key)

---

### Full Page Writes (FPW)

Writing complete page images to WAL after each checkpoint. Protects against partial page writes during crash.

**Control:**
- `full_page_writes` parameter
- `Insert->fullPageWrites` shared state
- `fpw_lsn` in `XLogInsertRecord`

---

### Replication Slot

A persistent marker that prevents WAL cleanup beyond a certain LSN, ensuring standbys can catch up. The `restart_lsn` advances as standbys confirm progress.

**Related:** [Chapter 6](06_standby_response.md#step-5-advance-replication-slot)

---

### Condition Variable

A synchronization primitive for efficient wakeup. Used in:
- `wal_flush_cv` - Wake physical walsenders after flush
- `wal_replay_cv` - Wake logical walsenders after replay

**Pattern:**
```c
// Waiter
ConditionVariablePrepareToSleep(&cv);
WaitEventSetWait(...);
ConditionVariableCancelSleep();

// Signaler
ConditionVariableBroadcast(&cv);
```

**Related:** [Chapter 3](03_wal_persistence.md#walsndwakeupprocessrequests-function), [Chapter 4](04_walsender_transmission.md#walsndwait-function)

---

### Latch

A simple signaling mechanism for waking processes. Each backend has a `procLatch`. Setting a latch wakes the waiting process from `WaitLatch()`.

**Usage in sync rep:**
```c
// Walsender releases waiter
SetLatch(&(proc->procLatch));

// Backend waits
WaitLatch(MyLatch, WL_LATCH_SET, ...);
```

**Related:** [Chapter 7](07_sync_wait_release.md#syncrepwakequeue-function)

---

### CopyData Message

A PostgreSQL protocol message type ('d') used to transfer data during COPY operations and replication. Contains:
- Nested message type ('w' for WAL data, 'k' for keepalive, 'r' for reply)
- Message-specific payload

**Related:** [Chapter 4](04_walsender_transmission.md#message-format), [Figure 6](diagrams/06_send_data_structure.mermaid)

---

## Synchronization Terms

### Memory Barrier

CPU instruction ensuring memory operation ordering. Used when lock-free patterns require visibility guarantees.

**Types:**
- `pg_read_barrier()` - Ensure preceding reads complete before subsequent reads
- `pg_write_barrier()` - Ensure preceding writes complete before subsequent writes

**Usage in sync rep:**
- `SyncRepWakeQueue()` uses `pg_write_barrier()` before setting state
- `SyncRepWaitForLSN()` uses `pg_read_barrier()` after checking state

**Related:** [Chapter 7](07_sync_wait_release.md#memory-barrier-requirements)

---

### Spinlock

A lightweight lock for very short critical sections. Busy-waits (spins) until acquired. Used for protecting shared memory updates.

**Examples:**
- `insertpos_lck` - Protects CurrBytePos/PrevBytePos
- `info_lck` - Protects XLogCtl shared variables
- `WalSnd.mutex` - Protects per-walsender state

**Related:** [Chapter 2](02_wal_generation_lsn.md#reservexloginsertlocation-function)

---

### LWLock (Lightweight Lock)

PostgreSQL's internal lock mechanism for protecting shared memory. Supports exclusive and shared modes.

**Key locks:**
- `WALWriteLock` - Serializes WAL write/fsync
- `WALInsertLocks[]` - 8 locks for concurrent WAL insertion
- `SyncRepLock` - Protects sync rep queue operations

**Related:** [Chapter 1](01_architecture_overview.md#lock-hierarchy)

---

## Configuration Terms

### synchronous_commit

Controls transaction durability level:

| Value | Durability | Latency |
|-------|------------|---------|
| `off` | May lose recent commits on crash | Lowest |
| `local` | Durable on primary | Low |
| `remote_write` | Written (not fsynced) on standby | Medium |
| `on` | Fsynced on standby | Medium-High |
| `remote_apply` | Applied/replayed on standby | Highest |

**Related:** [Appendix C](appendix_config_params.md#synchronous_commit)

---

### synchronous_standby_names

Specifies which standbys are synchronous and the confirmation method:

**Syntax:**
```
FIRST N (standby1, standby2, ...)  -- Priority mode
ANY N (standby1, standby2, ...)    -- Quorum mode
```

**Related:** [Chapter 7](07_sync_wait_release.md#synchronous_standby_names-syntax), [Appendix C](appendix_config_params.md#synchronous_standby_names)

---

## Navigation

[Index](index.md)
