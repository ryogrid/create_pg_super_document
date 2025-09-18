# ExecReScanCteScan

## Location
[src/backend/executor/nodeCtescan.c:307-339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeCtescan.c#L307-L339)

## Overview
Rescans a Common Table Expression (CTE) scan node by either clearing the underlying tuplestore or rewinding the read pointer, depending on whether the underlying CTE needs to be rescanned.

## Definition
```c
void ExecReScanCteScan(CteScanState *node)
```

## Detailed Description
ExecReScanCteScan handles the rescan operation for CTE scan nodes in PostgreSQL's executor. The function implements an intelligent rescan strategy that optimizes performance by avoiding unnecessary work when the underlying CTE data hasn't changed.

The function checks if the underlying CTE plan state has changed parameters (chgParam != NULL). If parameters have changed, it completely clears the tuplestore, which implicitly resets all read pointers and marks the CTE as not end-of-file. If no parameters have changed, it simply rewinds the current node's read pointer to reread existing data from the tuplestore.

This dual approach allows multiple CTE scan nodes to efficiently share the same tuplestore while ensuring that parameter changes trigger appropriate data refreshes.

## Parameters / Member Variables
- `node`: Pointer to the CteScanState structure representing the CTE scan node to be rescanned

## Dependencies
- Functions called/Symbols referenced:
  - ExecClearTuple
  - [ExecScanReScan](ExecScanReScan.md)
  - tuplestore_clear
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md)
  - [tuplestore_rescan](../t/tuplestore_rescan.md)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (general executor rescan dispatcher)

## Notes and Other Information
- Multiple CTE nodes may redundantly clear the same tuplestore, which is acceptable and not expensive
- The function handles both scenarios: when the underlying CTE needs rescanning and when it can reuse existing tuplestore data
- Parameter changes (chgParam) are used as the key indicator for determining whether a full rescan is needed
- The tuplestore clear operation implicitly resets all read pointers, making it safe for multiple concurrent CTE scan nodes