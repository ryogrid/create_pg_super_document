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
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)
  - [IsInParallelMode](../I/IsInParallelMode.md)
- Called from (representative examples):
  - [BeginCopyTo](../B/BeginCopyTo.md)
  - [ExecCreateTableAs](../E/ExecCreateTableAs.md)
  - [ExplainOnePlan](../E/ExplainOnePlan.md)
  - [refresh_matview_datafill](../r/refresh_matview_datafill.md)
  - [fmgr_sql](../f/fmgr_sql.md)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md)
  - [PortalRunMulti](../P/PortalRunMulti.md)

## Notes and Other Information
- Requires the active snapshot to have exactly one active reference (active_count == 1)
- Requires the active snapshot to have no registered references (regd_count == 0)
- Prevents modification during parallel operations to maintain consistency across workers
- Typically called after PushCopiedSnapshot to ensure the copied snapshot has the current command ID
- Used in scenarios where command execution requires up-to-date visibility rules

## Simplified Source

```c
// Simplified version of UpdateActiveSnapshotCommandId
void UpdateActiveSnapshotCommandId(void) {
    CommandId save_curcid, curcid;

    // Validate active snapshot state
    Assert(ActiveSnapshot != NULL);
    Assert(ActiveSnapshot->as_snap->active_count == 1);
    Assert(ActiveSnapshot->as_snap->regd_count == 0);

    // Get current command ID
    save_curcid = ActiveSnapshot->as_snap->curcid;
    curcid = GetCurrentCommandId(false);

    // Prevent modification during parallel operations
    if (IsInParallelMode() && save_curcid != curcid) {
        elog(ERROR, "cannot modify commandid in active snapshot during a parallel operation");
    }

    // Update snapshot's command ID
    ActiveSnapshot->as_snap->curcid = curcid;
}
```

Key simplifications made:
- Preserved essential command ID update logic
- Maintained critical validation assertions
- Kept parallel mode safety check
- Focused on core snapshot modification functionality