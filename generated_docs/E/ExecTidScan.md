# ExecTidScan

## Location
[src/backend/executor/nodeTidscan.c:433-446](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTidscan.c#L433-L446)

## Overview
ExecTidScan executes a TID (tuple identifier) scan operation, retrieving the next qualifying tuple from a relation using specific tuple identifiers.

## Definition
```c
static TupleTableSlot *
ExecTidScan(PlanState *pstate)
```

## Detailed Description
ExecTidScan is the main execution function for TID scan nodes in PostgreSQL's executor. It scans a relation using tuple identifiers (TIDs) and returns the next qualifying tuple in the specified direction. The function acts as a wrapper around the generic ExecScan() routine, providing it with TID-specific access methods (TidNext for fetching tuples and TidRecheck for rechecking conditions). The scan maintains a cursor position and operates under the assumption that the relation is already opened for scanning with the cursor positioned appropriately.

## Parameters / Member Variables
- `pstate`: PlanState pointer that gets cast to TidScanState, containing the execution state for the TID scan operation

## Dependencies
- Functions called/Symbols referenced:
  - castNode (to cast PlanState to TidScanState)
  - [ExecScan](ExecScan.md) (generic scan execution function)
  - [TidNext](../T/TidNext.md) (TID-specific tuple fetching function)
  - [TidRecheck](../T/TidRecheck.md) (TID-specific tuple recheck function)
- Called from (representative examples):
  - [ExecInitTidScan](ExecInitTidScan.md) (during TID scan node initialization)

## Notes and Other Information
- The function assumes the cursor is positioned at the previously returned tuple
- Initial state requires the relation to be opened for scanning with cursor positioned before the first qualifying tuple
- Uses the generic ExecScan framework with TID-specific access methods for consistency with other scan types
- The tss_TidPtr should be initialized to -1 in initial states

## Simplified Source

```c
static TupleTableSlot *
ExecTidScan(PlanState *pstate)
{
    // Cast plan state to TID scan specific state
    TidScanState *node = castNode(TidScanState, pstate);

    // Execute scan using TID-specific access methods
    // TidNext: fetches next tuple by TID
    // TidRecheck: rechecks tuple conditions
    return ExecScan(&node->ss, TidNext, TidRecheck);
}
```

This function is a simple wrapper that:
1. Converts the generic plan state to TID scan state
2. Delegates to the generic ExecScan framework with TID-specific methods
3. Returns the next qualifying tuple from the TID scan operation