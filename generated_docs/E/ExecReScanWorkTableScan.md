# ExecReScanWorkTableScan

## Location
[src/backend/executor/nodeWorktablescan.c:191-201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWorktablescan.c#L191-L201)

## Overview  
ExecReScanWorkTableScan resets a worktable scan to restart from the beginning, clearing any cached state and repositioning the tuplestore scan to the start.

## Definition
void ExecReScanWorkTableScan(WorkTableScanState *node)

## Detailed Description
ExecReScanWorkTableScan implements the rescan functionality for WorkTableScan plan nodes, allowing the scan to be restarted from the beginning. The function performs a multi-step reset process: first clearing any result tuple slot to ensure no stale cached tuples remain, then calling the generic scan rescan logic to reset scan-level state, and finally repositioning the underlying tuplestore to the beginning if the worktable has been initialized. The function includes a safety check to avoid attempting to rescan the tuplestore if the node hasn't been fully initialized yet (when rustate is NULL), which can occur in certain execution scenarios where rescan is called before the first execution.

## Parameters / Member Variables
- `node`: WorkTableScanState pointer containing the scan state to reset

## Dependencies
- Functions called/Symbols referenced:
  - [ExecClearTuple](ExecClearTuple.md) (clears cached tuple from result slot)
  - [ExecScanReScan](ExecScanReScan.md) (generic scan rescan functionality)
  - [tuplestore_rescan](../t/tuplestore_rescan.md) (resets tuplestore position to beginning)
- Types used:
  - [WorkTableScanState](../W/WorkTableScanState.md) (scan execution state)
- Called from:
  - [ExecReScan](ExecReScan.md) (generic plan node rescan dispatcher)

## Notes and Other Information
- Only rescans the tuplestore if the node has been fully initialized (rustate != NULL)
- Clears result tuple slot to prevent stale cached data
- Integrates with PostgreSQL's generic rescan framework
- Safe to call before first execution due to initialization check
- Essential for scenarios where the same worktable needs to be scanned multiple times
- Does not recreate or modify the tuplestore contents, only repositions the scan

## Simplified Source

```c
void ExecReScanWorkTableScan(WorkTableScanState *node) {
    // Clear any cached result tuple
    if (node->ss.ps.ps_ResultTupleSlot)
        ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

    // Reset generic scan state
    ExecScanReScan(&node->ss);

    // Rewind tuplestore to beginning if it's been initialized
    if (node->rustate)
        tuplestore_rescan(node->rustate->working_table);
}
```