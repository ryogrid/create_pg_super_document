# PushActiveSnapshot

## Location
[src/backend/utils/time/snapmgr.c:648-661](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L648-L661)

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
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [PushActiveSnapshotWithLevel](PushActiveSnapshotWithLevel.md)
- Called from (representative examples):
  - [ParallelWorkerMain](ParallelWorkerMain.md)
  - [RemoveTempRelationsCallback](../R/RemoveTempRelationsCallback.md)
  - [cluster_multiple_rels](../c/cluster_multiple_rels.md)
  - [EventTriggerOnLogin](../E/EventTriggerOnLogin.md)
  - [execute_sql_string](../e/execute_sql_string.md)
  - [ExecuteCallStmt](../E/ExecuteCallStmt.md)
  - [DefineIndex](../D/DefineIndex.md)
  - [ReindexMultipleInternal](../R/ReindexMultipleInternal.md)
  - [PersistHoldablePortal](PersistHoldablePortal.md)
  - [PreCommit_on_commit_actions](PreCommit_on_commit_actions.md)
  - [ATExecDetachPartition](../A/ATExecDetachPartition.md)
  - [AfterTriggerFireDeferred](../A/AfterTriggerFireDeferred.md)
  - [vacuum](../v/vacuum.md) operations
  - SPI operations
  - [Portal](Portal.md) operations
  - Many other locations throughout PostgreSQL

## Notes and Other Information
- This is a public function exported via snapmgr.h
- Commonly used when establishing snapshot context for operations that need consistent visibility
- The actual snapshot management logic is implemented in PushActiveSnapshotWithLevel
- Used extensively throughout PostgreSQL for establishing snapshot contexts in various subsystems
- Part of the snapshot stack management system - should be paired with PopActiveSnapshot when done

## Simplified Source

```c
// Simplified version of PushActiveSnapshot
void PushActiveSnapshot(Snapshot snapshot) {
    // Push snapshot at current transaction nesting level
    // Delegates all the complex logic to PushActiveSnapshotWithLevel
    PushActiveSnapshotWithLevel(snapshot, GetCurrentTransactionNestLevel());
}
```

Key simplifications made:
- This function is already very simple - it's just a wrapper
- The core functionality is delegating to PushActiveSnapshotWithLevel with the current transaction level
- All snapshot copying, reference counting, and stack management is handled by the delegated function
- Maintains the essential logic flow while abstracting the complexity to the underlying implementation