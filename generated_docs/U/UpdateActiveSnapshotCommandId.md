# UpdateActiveSnapshotCommandId

## Location
[src/backend/utils/time/snapmgr.c:712-742](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L712-L742)

## Overview
Updates the command ID of the active snapshot to the current command ID, ensuring visibility consistency during command execution.

## Definition

```c
void
UpdateActiveSnapshotCommandId(void)
```
## Detailed Description
UpdateActiveSnapshotCommandId updates the current command ID (curcid) of the active snapshot to match the current transaction's command ID. This function ensures that the snapshot reflects the correct visibility rules for the current command context. The function includes several safety checks: it verifies that the active snapshot exists, has exactly one active reference count, and has zero registered reference count (meaning it's not shared elsewhere). Additionally, it prevents modification during parallel operations to maintain consistency across worker processes, as snapshots are shared at the beginning of parallel operations.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - CommandId (type)
  - GetCurrentCommandId
  - IsInParallelMode
- Called from (representative examples):
  - BeginCopyTo
  - ExecCreateTableAs
  - ExplainOnePlan
  - refresh_matview_datafill
  - fmgr_sql
  - _SPI_execute_plan
  - PortalRunMulti

## Notes and Other Information
- Requires the active snapshot to have exactly one active reference (active_count == 1)
- Requires the active snapshot to have no registered references (regd_count == 0)
- Prevents modification during parallel operations to maintain consistency across workers
- Typically called after PushCopiedSnapshot to ensure the copied snapshot has the current command ID
- Used in scenarios where command execution requires up-to-date visibility rules