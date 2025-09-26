# PopActiveSnapshot

## Location
src/backend/utils/time/snapmgr.c: 743 - 769

## Overview
Removes the topmost snapshot from the active snapshot stack, decrements its reference count, and frees it if no longer referenced.

## Definition

```c
void
PopActiveSnapshot(void)
```
## Detailed Description
PopActiveSnapshot removes the topmost snapshot from the active snapshot stack and performs proper cleanup. It decrements the active reference count of the snapshot and checks if the snapshot can be freed (when both active_count and regd_count reach zero). The function also updates global snapshot tracking variables: it sets ActiveSnapshot to point to the next snapshot in the stack, and if the stack becomes empty, it clears OldestActiveSnapshot as well. Finally, it calls SnapshotResetXmin() to recalculate the oldest snapshot's xmin value, which is crucial for determining the oldest transaction that might still be running.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ActiveSnapshotElt (type)
  - FreeSnapshot
  - SnapshotResetXmin
- Called from (representative examples):
  - ParallelWorkerMain
  - cluster_multiple_rels
  - EndCopyTo
  - ExecCreateTableAs
  - ExplainOnePlan
  - vacuum_rel
  - _SPI_execute_plan
  - PortalRunSelect
  - PortalRunMulti

## Notes and Other Information
- Automatically frees snapshots when both active_count and regd_count reach zero
- Updates global snapshot tracking variables (ActiveSnapshot, OldestActiveSnapshot)
- Calls SnapshotResetXmin() to maintain proper xmin tracking across all active snapshots
- Essential counterpart to PushActiveSnapshot and PushCopiedSnapshot
- Widely used across PostgreSQL for snapshot lifecycle management in various contexts
- Critical for preventing snapshot leaks and maintaining proper transaction isolation