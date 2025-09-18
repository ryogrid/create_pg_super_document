# heap_getnext

## Location
src/backend/access/heap/heapam.c: 1296 - 1344

## Overview
Retrieves the next tuple from a heap table scan, handling both page-mode and regular scanning while enforcing access method validation and transaction safety checks.

## Definition


## Detailed Description
The  function is the main interface for retrieving the next tuple during a heap table scan. It performs safety checks to ensure the scan is using the heap access method and validates transaction state for logical decoding scenarios. The function delegates the actual tuple retrieval to either  or  based on scan flags, then performs statistics tracking before returning the result.

The function includes important safety mechanisms: it validates that the relation is using the heap access method (allowing for regression testing with alternative AMs that reuse heap handlers), and prevents direct calls during logical decoding when  is valid, which could cause consistency issues.

## Parameters / Member Variables
- : The table scan descriptor (cast to HeapScanDesc internally)
- : The scan direction (ForwardScanDirection or BackwardScanDirection)

## Dependencies
- Functions called/Symbols referenced:
  - GetHeapamTableAmRoutine
  - heapgettup_pagemode
  - heapgettup
  - pgstat_count_heap_getnext
- Data structures used:
  - HeapScanDesc
  - TableScanDesc
  - ScanDirection
  - HeapTuple
- Scan flags:
  - SO_ALLOW_PAGEMODE
- Called from (representative examples):
  - heapam_index_build_range_scan
  - heapam_index_validate_scan
  - populate_typ_list
  - objectsInSchemaToOids
  - getRelationsInNamespace
  - get_tables_to_cluster
  - ReindexMultipleTables
  - get_all_vacuum_rels
  - do_autovacuum

## Notes and Other Information
- Includes a safety check to ensure only heap AM is supported, which can be downgraded to an assert in future versions
- The AM routine check (rather than AM OID check) allows regression tests to create alternative AMs that reuse heap handlers
- Prevents unexpected calls during logical decoding when  is valid to avoid transaction consistency issues
- No locking manipulations are needed as tuple-level locking is handled at lower levels
- Returns NULL if no tuple is found ()
- Performs statistics tracking via  for monitoring scan activity
- The function is widely used directly without going through the table AM interface, hence the safety checks