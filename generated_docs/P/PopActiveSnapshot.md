# PopActiveSnapshot

## Location
[src/backend/utils/time/snapmgr.c:743-769](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L743-L769)

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
  - [ActiveSnapshotElt](../A/ActiveSnapshotElt.md) (type)
  - [FreeSnapshot](../F/FreeSnapshot.md)
  - [SnapshotResetXmin](../S/SnapshotResetXmin.md)
- Called from (representative examples):
  - [ParallelWorkerMain](ParallelWorkerMain.md)
  - [cluster_multiple_rels](../c/cluster_multiple_rels.md)
  - [EndCopyTo](../E/EndCopyTo.md)
  - [ExecCreateTableAs](../E/ExecCreateTableAs.md)
  - [ExplainOnePlan](../E/ExplainOnePlan.md)
  - [vacuum_rel](../v/vacuum_rel.md)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md)
  - [PortalRunSelect](PortalRunSelect.md)
  - [PortalRunMulti](PortalRunMulti.md)

## Notes and Other Information
- Automatically frees snapshots when both active_count and regd_count reach zero
- Updates global snapshot tracking variables (ActiveSnapshot, OldestActiveSnapshot)
- Calls SnapshotResetXmin() to maintain proper xmin tracking across all active snapshots
- Essential counterpart to PushActiveSnapshot and PushCopiedSnapshot
- Widely used across PostgreSQL for snapshot lifecycle management in various contexts
- Critical for preventing snapshot leaks and maintaining proper transaction isolation