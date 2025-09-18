# index_getnext_tid

## Location
src/backend/access/index/indexam.c: 574 - 631

## Overview
The `index_getnext_tid` function retrieves the next tuple identifier (TID) from an index scan that satisfies the scan keys, serving as a core primitive for index-based tuple retrieval.

## Definition
```c
ItemPointer index_getnext_tid(IndexScanDesc scan, ScanDirection direction)
```

## Detailed Description
This function is a fundamental building block for index scanning operations. It delegates to the access method's `amgettuple` function to find the next index entry that matches the scan conditions. The process involves:

1. Validating the scan state and checking that required access method procedures are available
2. Calling the access method's tuple retrieval function (`amgettuple`) to get the next matching entry
3. Resetting safety flags (`kill_prior_tuple` and `xs_heap_continue`) immediately after the call
4. Handling end-of-scan conditions by cleaning up resources when no more tuples are found
5. Updating statistics for index usage tracking
6. Returning a pointer to the tuple identifier stored in the scan descriptor

The function returns NULL when no more matching tuples exist, indicating the end of the scan.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure representing the current index scan state
- `direction`: ScanDirection indicating whether to scan forward or backward through the index

## Dependencies
- Functions called/Symbols referenced:
  - `SCAN_CHECKS` (macro for scan validation)
  - `CHECK_SCAN_PROCEDURE` (macro to verify amgettuple procedure exists)
  - `table_index_fetch_reset` (resets heap fetch resources)
  - `[ItemPointerIsValid](../I/ItemPointerIsValid.md)` (validates tuple identifier)
  - `pgstat_count_index_tuples` (updates index usage statistics)
  - `ScanDirection` (enumeration type)
  - `[IndexScanDesc](../I/IndexScanDesc.md)` (scan descriptor type)
- Called from (representative examples):
  - `[index_getnext_slot](index_getnext_slot.md)` (src/backend/access/index/indexam.c:682)
  - `[IndexOnlyNext](../I/IndexOnlyNext.md)` (src/backend/executor/nodeIndexonlyscan.c:120)
  - `get_actual_variable_endpoint` (src/backend/utils/adt/selfuncs.c:6409)

## Notes and Other Information
- The function assumes a valid transaction snapshot is available (asserts RecentXmin is valid)
- Kill flags are reset immediately for safety to prevent accidental tuple marking
- Resources like buffer pins are automatically cleaned up when the scan completes
- The actual TID is stored in `scan->xs_heaptid` and a pointer to it is returned
- This function focuses only on retrieving TIDs; actual heap tuple fetching is handled separately
- Location: src/backend/access/index/indexam.c:574-631