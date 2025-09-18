# XLogCtlData

## Location
src/backend/access/transam/xlog.c: 451 - 555

## Overview
XLogCtlData is the master shared-memory control structure that contains all the global state for PostgreSQL's Write-Ahead Log (WAL) system, coordinating WAL operations across all backend processes.

## Definition


## Detailed Description
XLogCtlData serves as the central nervous system for PostgreSQL's WAL subsystem, containing all shared state necessary for coordinating WAL operations across multiple backend processes. This structure manages everything from WAL insertion and buffer management to recovery state tracking and checkpoint coordination.

The structure is carefully designed with different locking strategies for different types of data: the info_lck spinlock protects frequently accessed metadata, atomic operations handle high-contention counters, and specialized locks like WALWriteLock and WALBufMappingLock protect specific subsystems.

Key responsibilities include tracking WAL insertion progress, managing the WAL buffer cache, coordinating write and flush operations, maintaining timeline information for point-in-time recovery, and providing the infrastructure for crash recovery and online backup operations.

## Parameters / Member Variables
- : XLogCtlInsert structure managing WAL insertion operations and locking
- : Write and flush request tracking (protected by info_lck)
- : Recent copy of the current redo point for insertions
- : Transaction ID from the latest checkpoint
- : LSN of the most recent asynchronous commit or abort
- : Oldest LSN still needed by any replication slot
- : Most recently removed or recycled WAL segment number
- : Fake LSN counter for unlogged relations (atomic)
- : Timestamp of last WAL segment switch
- : LSN at last WAL segment switch
- : Last byte position + 1 inserted to buffers (atomic)
- : Last byte position + 1 written to disk (atomic)
- : Last byte position + 1 flushed to disk (atomic)
- : Latest initialized page position in WAL buffer cache
- : Buffer array for unwritten WAL pages
- : Array of block end positions for WAL buffers (atomic)
- : Highest allocated WAL buffer index
- : Timeline ID for current WAL insertion and flushing
- : Previous timeline ID before fork
- : Current recovery state (crash/archive recovery)
- : Controls WAL segment installation rights
- : Indicates if WAL writer is in low-power mode
- : Start position of last checkpoint record
- : End position + 1 of last checkpoint record
- : Copy of the latest checkpoint record
- : Start of last full-page-write disable record
- : Spinlock protecting shared variables

## Dependencies
- Functions called/Symbols referenced:
  - [XLogCtlInsert](XLogCtlInsert.md) (WAL insertion control structure)
  - [XLogwrtRqst](XLogwrtRqst.md) (write request structure)
  - FullTransactionId (transaction ID type)
  - XLogSegNo (WAL segment number type)
  - [pg_atomic_uint64](../p/pg_atomic_uint64.md) (atomic 64-bit integer type)
  - pg_time_t (time type)
  - [RecoveryState](../R/RecoveryState.md) (recovery state enumeration)
  - CheckPoint (checkpoint record structure)
  - [slock_t](../s/slock_t.md) (spinlock type)
- Called from (representative examples):
  - [WalInsertClass](../W/WalInsertClass.md)
  - [XLOGShmemSize](XLOGShmemSize.md)
  - [XLOGShmemInit](XLOGShmemInit.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)

## Notes and Other Information
- Central shared-memory structure for all WAL operations in PostgreSQL
- Uses multiple locking strategies optimized for different access patterns
- Critical for crash recovery, point-in-time recovery, and online backup functionality
- Timeline management enables branching recovery scenarios
- Atomic counters provide high-performance tracking of WAL progress
- Buffer management coordinates WAL page allocation and initialization
- Checkpoint tracking supports both crash recovery and performance optimization
- Replication slot integration ensures WAL retention for streaming replication
- Carefully designed memory layout and locking to minimize contention in high-concurrency environments