# GetActiveSnapshot

## Location
[src/backend/utils/time/snapmgr.c:770-781](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L770-L781)

## Overview
Returns a pointer to the topmost snapshot in the active snapshot stack.

## Definition

```c
Snapshot
GetActiveSnapshot(void)
```
## Detailed Description
GetActiveSnapshot provides access to the currently active snapshot by returning a pointer to the snapshot structure at the top of the active snapshot stack. This function is fundamental to PostgreSQL's MVCC (Multi-Version Concurrency Control) system, as it allows various parts of the system to access the current visibility rules for determining which tuples should be visible to the current operation. The function includes an assertion to ensure that an active snapshot exists before attempting to access it, preventing potential null pointer dereferences.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - None (accesses global ActiveSnapshot variable directly)
- Called from (representative examples):
  - [spgvacuumscan](../s/spgvacuumscan.md)
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md)
  - [BeginCopyTo](../B/BeginCopyTo.md)
  - [ExecCreateTableAs](../E/ExecCreateTableAs.md)
  - [ExplainOnePlan](../E/ExplainOnePlan.md)
  - [standard_ExecutorStart](../s/standard_ExecutorStart.md)
  - [standard_ExecutorRun](../s/standard_ExecutorRun.md)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md)
  - [ProcessQuery](../P/ProcessQuery.md)
  - [PortalStart](../P/PortalStart.md)

## Notes and Other Information
- Returns a direct pointer to the snapshot structure, not a copy
- Callers should not modify the returned snapshot unless it was created via PushCopiedSnapshot
- Essential for MVCC operations throughout the PostgreSQL system
- Used extensively in executor, planner, and utility command contexts
- The returned snapshot determines tuple visibility for the current operation
- Must be called only when an active snapshot exists (enforced by assertion)