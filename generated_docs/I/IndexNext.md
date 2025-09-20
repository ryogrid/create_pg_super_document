# IndexNext

## Location
[src/backend/executor/nodeIndexscan.c:80-167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexscan.c#L80-L167)

## Overview
The IndexNext function retrieves a tuple from the IndexScan node's current relation using the index specified in the IndexScanState information.

## Definition

```c
static TupleTableSlot *
IndexNext(IndexScanState *node)
```
## Detailed Description
IndexNext is a core function in PostgreSQL's index scan execution that performs the actual tuple retrieval from an index. It handles the complete workflow of index scanning including:

1. **Initialization**: Extracts necessary information from the index scan node including estate, scan direction, and expression context
2. **Direction Management**: Combines the plan's scan direction with the current execution direction using ScanDirectionCombine
3. **Scan Descriptor Setup**: If no scan descriptor exists, it initializes one using index_beginscan and optionally calls index_rescan if runtime keys are ready
4. **Tuple Retrieval Loop**: Continuously fetches tuples using index_getnext_slot until a valid tuple is found or the scan ends
5. **Lossy Index Handling**: For lossy indexes, it performs recheck qualification using the original index quals to ensure the tuple actually matches the scan conditions
6. **End-of-Scan Management**: Sets the ReachedEnd flag when no more tuples are available

The function implements a filter-and-recheck pattern that is essential for handling lossy index access methods where the index may return false positives.

## Parameters / Member Variables
- : IndexScanState structure containing all the state information for the index scan operation, including scan descriptors, keys, and tuple slots

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionCombine
  - [index_beginscan](../i/index_beginscan.md)
  - [index_rescan](../i/index_rescan.md)
  - [index_getnext_slot](../i/index_getnext_slot.md)
  - ExecQualAndReset
  - InstrCountFiltered2
  - ExecClearTuple
  - CHECK_FOR_INTERRUPTS
- Called from (representative examples):
  - ReorderTuple (nodeIndexscan.c:58)
  - [ExecIndexScan](../E/ExecIndexScan.md) (nodeIndexscan.c:535)

## Notes and Other Information
- This is a static function used internally within the index scan executor
- Handles both parallel and non-parallel index scans
- Implements proper interruption checking for long-running scans
- The lossy index recheck mechanism is crucial for maintaining query correctness with approximate index access methods
- The function properly manages the end-of-scan state to prevent infinite loops
- Runtime key evaluation is deferred until the keys are ready, allowing for parameter-dependent scan optimization