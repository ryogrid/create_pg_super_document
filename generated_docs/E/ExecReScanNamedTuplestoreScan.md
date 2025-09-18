# ExecReScanNamedTuplestoreScan

## Location
[src/backend/executor/nodeNamedtuplestorescan.c:164-178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeNamedtuplestorescan.c#L164-L178)

## Overview
Resets a named tuple store scan to the beginning, clearing any cached results and repositioning the read pointer for a fresh scan of the tuple store.

## Definition
```c
void
ExecReScanNamedTuplestoreScan(NamedTuplestoreScanState *node)
```

## Detailed Description
ExecReScanNamedTuplestoreScan implements the rescan functionality for NamedTuplestoreScan nodes, allowing the scan to be reset to its initial state and re-executed from the beginning. The function first clears any cached result tuple from the result tuple slot to ensure fresh results, then calls the generic ExecScanReScan to handle standard scan state reset operations.

The core rescan operation involves selecting the node's specific read pointer and rewinding the tuple store to its beginning position. This ensures that subsequent calls to the scan will start from the first tuple in the store, effectively implementing the rescan semantics required by PostgreSQL's executor framework.

This function is essential for supporting operations that require multiple passes over the same data, such as certain join algorithms, nested loop operations, or when a plan node needs to be re-executed due to parameter changes.

## Parameters / Member Variables
- `node`: Pointer to NamedTuplestoreScanState containing the scan state, tuple store reference, and read pointer to be reset

## Dependencies
- Functions called/Symbols referenced:
  - ExecClearTuple: Clears any cached tuple from the result tuple slot
  - [ExecScanReScan](ExecScanReScan.md): Performs generic scan state reset operations
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md): Selects the node's read pointer for operations
  - [tuplestore_rescan](../t/tuplestore_rescan.md): Rewinds the tuple store to the beginning position
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md): Generic rescan dispatcher in the executor framework

## Notes and Other Information
- Function returns void as it performs state modification rather than returning data
- Clears the result tuple slot only if it exists (ps_ResultTupleSlot check)
- Rewinds the node's specific read pointer, not affecting other potential readers of the tuple store
- Part of PostgreSQL's standard executor rescan protocol
- Essential for supporting multiple passes over CTE data in complex query plans
- Ensures clean state for re-execution without data corruption or stale cached results
- Works in conjunction with the standard executor framework's rescan mechanisms