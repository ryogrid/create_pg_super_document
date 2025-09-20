# heap_getnext

## Location
[src/backend/access/heap/heapam.c:1296-1344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L1296-L1344)

## Overview
Retrieves the next tuple from a heap table scan, handling both page-mode and regular scanning while enforcing access method validation and transaction safety checks.

## Definition

```c
HeapTuple
heap_getnext(TableScanDesc sscan, ScanDirection direction)
```
## Detailed Description
The  function is the main interface for retrieving the next tuple during a heap table scan. It performs safety checks to ensure the scan is using the heap access method and validates transaction state for logical decoding scenarios. The function delegates the actual tuple retrieval to either  or  based on scan flags, then performs statistics tracking before returning the result.

The function includes important safety mechanisms: it validates that the relation is using the heap access method (allowing for regression testing with alternative AMs that reuse heap handlers), and prevents direct calls during logical decoding when  is valid, which could cause consistency issues.

## Parameters / Member Variables
- : The table scan descriptor (cast to HeapScanDesc internally)
- : The scan direction (ForwardScanDirection or BackwardScanDirection)

## Dependencies
- Functions called/Symbols referenced:
  - [GetHeapamTableAmRoutine](../G/GetHeapamTableAmRoutine.md)
  - [heapgettup_pagemode](heapgettup_pagemode.md)
  - [heapgettup](heapgettup.md)
  - pgstat_count_heap_getnext
- Data structures used:
  - [HeapScanDesc](../H/HeapScanDesc.md)
  - [TableScanDesc](../T/TableScanDesc.md)
  - ScanDirection
  - HeapTuple
- Scan flags:
  - SO_ALLOW_PAGEMODE
- Called from (representative examples):
  - [heapam_index_build_range_scan](heapam_index_build_range_scan.md)
  - [heapam_index_validate_scan](heapam_index_validate_scan.md)
  - [populate_typ_list](../p/populate_typ_list.md)
  - [objectsInSchemaToOids](../o/objectsInSchemaToOids.md)
  - [getRelationsInNamespace](../g/getRelationsInNamespace.md)
  - [get_tables_to_cluster](../g/get_tables_to_cluster.md)
  - [ReindexMultipleTables](../R/ReindexMultipleTables.md)
  - [get_all_vacuum_rels](../g/get_all_vacuum_rels.md)
  - [do_autovacuum](../d/do_autovacuum.md)

## Notes and Other Information
- Includes a safety check to ensure only heap AM is supported, which can be downgraded to an assert in future versions
- The AM routine check (rather than AM OID check) allows regression tests to create alternative AMs that reuse heap handlers
- Prevents unexpected calls during logical decoding when  is valid to avoid transaction consistency issues
- No locking manipulations are needed as tuple-level locking is handled at lower levels
- Returns NULL if no tuple is found ()
- Performs statistics tracking via  for monitoring scan activity
- The function is widely used directly without going through the table AM interface, hence the safety checks