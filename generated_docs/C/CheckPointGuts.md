# CheckPointGuts

## Location
src/backend/access/transam/xlog.c: 7504 - 7543

## Overview
Performs the core checkpoint work by systematically flushing all dirty data from shared memory to disk across multiple PostgreSQL subsystems.

## Definition


## Detailed Description
CheckPointGuts contains the essential data flushing logic shared between regular checkpoints and recovery restartpoints. The function orchestrates a comprehensive flush of all dirty data structures in PostgreSQL, ensuring data durability across multiple subsystems.

The function operates in a carefully ordered sequence: first handling metadata and control structures (relation map, replication slots, snapshots), then flushing all SLRU (Simple LRU) caches and the main buffer pool, followed by processing all queued fsync requests to ensure physical disk writes are completed, and finally handling two-phase commit checkpointing.

The ordering is critical for consistency - two-phase commit checkpointing is deliberately delayed until the end to ensure all related data modifications are safely on disk before recording the checkpoint state of prepared transactions.

## Parameters / Member Variables
- `checkPointRedo`: The WAL LSN from which recovery would begin if needed after this checkpoint
- `flags`: Checkpoint control flags, primarily used to determine if this is a shutdown checkpoint

## Dependencies
- Functions called/Symbols referenced:
  - CheckPointRelationMap (relation mapping tables)
  - CheckPointReplicationSlots (replication slot state)
  - CheckPointSnapBuild (snapshot building state)
  - CheckPointLogicalRewriteHeap (logical replication heap)
  - CheckPointReplicationOrigin (replication origin state)
  - CheckPointCLOG (commit log SLRU)
  - CheckPointCommitTs (commit timestamp SLRU)
  - CheckPointSUBTRANS (subtransaction SLRU)
  - CheckPointMultiXact (multixact SLRU)
  - CheckPointPredicate (predicate lock state)
  - CheckPointBuffers (main buffer pool)
  - ProcessSyncRequests (fsync request processing)
  - CheckPointTwoPhase (two-phase commit state)
  - GetCurrentTimestamp (timing statistics)
- Called from (representative examples):
  - CreateCheckPoint
  - CreateRestartPoint
  - RefreshXLogWriteResult

## Notes and Other Information
- Shared implementation between regular checkpoints and recovery restartpoints
- Includes comprehensive performance instrumentation with timing statistics
- Two-phase commit checkpointing is intentionally performed last for consistency
- Handles both SLRU (Simple LRU) subsystems and the main buffer pool
- ProcessSyncRequests ensures all pending fsync operations complete before returning
- Order of operations is critical for maintaining data consistency guarantees
- Performance tracing points enable monitoring of checkpoint I/O patterns
- The function represents the most I/O-intensive part of checkpoint processing