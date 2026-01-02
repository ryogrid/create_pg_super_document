# PostgreSQL 17.6 Synchronous Streaming Replication

## Technical Documentation

**Version:** 17.6
**Analysis Date:** 2026-01-03
**Coverage:** WAL Generation through Client Commit Response

---

## Executive Summary

PostgreSQL's synchronous streaming replication ensures that transaction commits are durably replicated to one or more standby servers before acknowledging success to the client. This documentation provides a comprehensive analysis of the complete data flow, from WAL record generation through client commit response.

### Complete Data Flow Overview

The synchronous replication process follows eight distinct phases:

```
Client COMMIT -> WAL Insert -> WAL Flush -> Walsender Transmission
     -> Standby Receipt -> Reply Processing -> Backend Release -> Client Response
```

**Phase 1 - WAL Generation (Chapter 2):**
Backend processes insert commit records into shared WAL buffers via `XLogInsert()` and `XLogInsertRecord()`. LSN assignment occurs atomically under the `insertpos_lck` spinlock, using a "byte position" abstraction for efficiency.

**Phase 2 - WAL Persistence (Chapter 3):**
`XLogFlush()` ensures WAL is durably written to disk. The group commit optimization via `LWLockAcquireOrWait()` allows multiple transactions to share a single fsync operation.

**Phase 3 - Walsender Transmission (Chapter 4):**
Walsender processes (`WalSndLoop()`) transmit WAL to standbys via `XLogSendPhysical()`. Data is read from WAL buffers (lock-free) or files, packaged into CopyData messages up to 16MB.

**Phase 4 - Standby Receipt:**
Walreceiver writes incoming WAL to disk and optionally fsyncs based on configuration.

**Phase 5 - Standby Reply (Chapter 6):**
Standby sends position updates (write, flush, apply LSNs) back to the primary. The walsender's `ProcessStandbyReplyMessage()` updates shared memory state.

**Phase 6 - Sync Rep Wait/Release (Chapter 7):**
Backends wait in `SyncRepWaitForLSN()` on LSN-ordered queues. Walsenders release waiters via `SyncRepReleaseWaiters()` when sufficient standbys confirm the LSN.

**Phase 7 - Client Response (Chapter 8):**
Upon release, backends complete commit cleanup and return success to the client.

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Byte position abstraction | Minimizes spinlock hold time during LSN assignment |
| Multiple WAL insertion locks | Allows concurrent WAL insertions (default: 8 locks) |
| LWLockAcquireOrWait pattern | Enables group commit without explicit coordination |
| Atomic progress tracking | Lock-free progress monitoring for walsenders |
| LSN-ordered wait queues | Efficient batch release of waiting backends |
| Condition variable wakeup | Eliminates walsender polling overhead |

### Performance Characteristics

- **Commit latency:** Network RTT + standby flush time (typically 1-100ms)
- **Throughput:** Limited by fsync rate and network bandwidth
- **Scalability:** Multiple WAL insertion locks reduce contention
- **Group commit:** Amortizes fsync cost across concurrent transactions

---

## Table of Contents

### Core Documentation

1. **[Architecture Overview](01_architecture_overview.md)**
   - System components and their interactions
   - Shared memory structures
   - Lock hierarchy and synchronization
   - Complete data flow diagram

2. **[WAL Generation and LSN Assignment](02_wal_generation_lsn.md)**
   - XLogInsert and XLogInsertRecord internals
   - LSN assignment via ReserveXLogInsertLocation
   - Byte position to XLogRecPtr conversion
   - WAL insertion lock distribution

3. **[WAL Persistence](03_wal_persistence.md)**
   - XLogFlush and XLogWrite implementation
   - Group commit via LWLockAcquireOrWait
   - Write/sync range determination
   - Atomic progress tracking

4. **[Walsender Transmission](04_walsender_transmission.md)**
   - WalSndLoop main processing
   - XLogSendPhysical data packaging
   - WAL buffer lock-free reading
   - Message format and MAX_SEND_SIZE

5. **[Keepalive and Monitoring](05_keepalive_monitoring.md)**
   - WalSndKeepalive protocol
   - Timeout detection mechanism
   - Reply timestamp tracking

6. **[Standby Response Processing](06_standby_response.md)**
   - ProcessStandbyReplyMessage internals
   - Write/flush/apply position tracking
   - Lag calculation via LagTracker

7. **[Synchronous Replication Wait and Release](07_sync_wait_release.md)**
   - SyncRepWaitForLSN wait mechanism
   - SyncRepReleaseWaiters release logic
   - Queue management and memory barriers

8. **[Client Response and Commit Completion](08_client_response.md)**
   - Wake and completion sequence
   - Interrupt handling during wait
   - Complete transaction timeline

### Appendices

- **[Appendix A: Symbol Index](appendix_symbol_index.md)** - Alphabetical listing of all documented symbols
- **[Appendix B: Glossary](appendix_glossary.md)** - Technical terminology definitions
- **[Appendix C: Configuration Parameters](appendix_config_params.md)** - Complete parameter reference

### Diagrams

All diagrams are in Mermaid format and can be rendered with compatible viewers.

| # | Diagram | Description |
|---|---------|-------------|
| 1 | [Overall Architecture](diagrams/01_overall_architecture.mermaid) | Complete system component overview |
| 2 | [LSN Assignment Sequence](diagrams/02_lsn_assignment_sequence.mermaid) | Detailed XLogInsertRecord flow |
| 3 | [WAL Write/Sync Flow](diagrams/03_wal_write_sync_flow.mermaid) | XLogFlush and XLogWrite operation |
| 4 | [WAL Buffer States](diagrams/04_wal_buffer_state.mermaid) | Buffer lifecycle transitions |
| 5 | [Walsender State Machine](diagrams/05_walsender_state.mermaid) | WalSndState transitions |
| 6 | [Send Data Structure](diagrams/06_send_data_structure.mermaid) | CopyData message format |
| 7 | [Walsender Iteration](diagrams/07_walsender_iteration.mermaid) | Single WalSndLoop iteration |
| 8 | [Standby Response](diagrams/08_standby_response_sequence.mermaid) | Reply processing sequence |
| 9 | [Sync Wait/Release](diagrams/09_sync_wait_release_sequence.mermaid) | Backend wait and wakeup |
| 10 | [SyncRepQueue States](diagrams/10_syncrep_queue_state.mermaid) | Queue state transitions |
| 11 | [Complete Commit Sequence](diagrams/11_complete_commit_sequence.mermaid) | End-to-end commit flow |

---

## Quick Reference

### Critical Code Paths

**WAL Insertion Path:**
```
XLogInsert -> XLogRecordAssemble -> XLogInsertRecord ->
WALInsertLockAcquire -> ReserveXLogInsertLocation ->
CopyXLogRecordToWAL -> WALInsertLockRelease
```

**WAL Flush Path:**
```
XLogFlush -> WaitXLogInsertionsToFinish ->
LWLockAcquireOrWait(WALWriteLock) -> XLogWrite ->
issue_xlog_fsync -> WalSndWakeupProcessRequests
```

**Synchronous Replication Wait Path:**
```
SyncRepWaitForLSN -> SyncRepQueueInsert -> WaitLatch
```

**WAL Send Path:**
```
WalSndLoop -> XLogSendPhysical -> GetFlushRecPtr ->
WALReadFromBuffers -> pq_putmessage_noblock
```

**Standby Reply Processing Path:**
```
ProcessRepliesIfAny -> ProcessStandbyMessage ->
ProcessStandbyReplyMessage -> SyncRepReleaseWaiters ->
SyncRepWakeQueue -> SetLatch
```

### Key Source Files

| File | Purpose |
|------|---------|
| `src/backend/access/transam/xlog.c` | WAL insertion, write, flush |
| `src/backend/access/transam/xloginsert.c` | Record assembly |
| `src/backend/replication/walsender.c` | Walsender process |
| `src/backend/replication/syncrep.c` | Synchronous replication |
| `src/include/replication/walsender_private.h` | Shared memory structures |

### Shared Memory Structures

| Structure | Location | Purpose |
|-----------|----------|---------|
| XLogCtlData | xlog.c:451 | Central WAL management |
| XLogCtlInsert | xlog.c:397 | Insertion coordination |
| WalSndCtlData | walsender_private.h:91 | Walsender control |
| WalSnd | walsender_private.h:42 | Per-walsender state |

---

## How to Use This Documentation

**For Understanding the System:**
Start with Chapter 1 (Architecture Overview) and the Complete Commit Sequence diagram (Figure 11) to understand the overall flow. Then proceed sequentially through chapters 2-8.

**For Debugging Issues:**
Use the Symbol Index (Appendix A) to locate specific functions. Cross-references within each chapter point to related concepts.

**For Configuration Tuning:**
Appendix C provides a complete parameter reference with impact analysis.

**For Code Navigation:**
Each chapter includes file:line references for source code locations in PostgreSQL 17.6.

---

*Documentation generated from PostgreSQL 17.6 source code analysis.*
