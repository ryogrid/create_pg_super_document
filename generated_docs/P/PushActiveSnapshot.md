# PushActiveSnapshot

## Location
src/backend/utils/time/snapmgr.c: 648 - 661

## Overview
Sets the given snapshot as the current active snapshot, handling reference counting and copying as needed for snapshot lifetime management.

## Definition
```c
void PushActiveSnapshot(Snapshot snapshot)
```

## Detailed Description
This function serves as a simplified interface to PushActiveSnapshotWithLevel, automatically using the current transaction nesting level. It establishes a snapshot as the active snapshot for subsequent operations, managing the snapshot's lifetime through reference counting.

The function delegates to PushActiveSnapshotWithLevel which handles the complexity of:
- Determining whether the snapshot needs to be copied (for statically-allocated or command-counter-sensitive snapshots)
- Managing reference counts appropriately  
- Maintaining the active snapshot stack

This is the standard interface used throughout PostgreSQL code when pushing snapshots without needing to specify a particular transaction nesting level.

## Parameters / Member Variables
- `snapshot`: The snapshot to make active, can be static or dynamic

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTransactionNestLevel
  - PushActiveSnapshotWithLevel
- Called from (representative examples):
  - ParallelWorkerMain
  - RemoveTempRelationsCallback
  - cluster_multiple_rels
  - EventTriggerOnLogin
  - execute_sql_string
  - ExecuteCallStmt
  - DefineIndex
  - ReindexMultipleInternal
  - PersistHoldablePortal
  - PreCommit_on_commit_actions
  - ATExecDetachPartition
  - AfterTriggerFireDeferred
  - vacuum operations
  - SPI operations
  - Portal operations
  - Many other locations throughout PostgreSQL

## Notes and Other Information
- This is a public function exported via snapmgr.h
- Commonly used when establishing snapshot context for operations that need consistent visibility
- The actual snapshot management logic is implemented in PushActiveSnapshotWithLevel
- Used extensively throughout PostgreSQL for establishing snapshot contexts in various subsystems
- Part of the snapshot stack management system - should be paired with PopActiveSnapshot when done