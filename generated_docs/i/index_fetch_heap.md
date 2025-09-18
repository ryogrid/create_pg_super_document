# index_fetch_heap

## Location
src/backend/access/index/indexam.c: 632 - 672

## Overview
The `index_fetch_heap` function retrieves the actual heap tuple corresponding to the most recently obtained index TID, handling MVCC visibility and HOT chain traversal.

## Definition
```c
bool index_fetch_heap(IndexScanDesc scan, TupleTableSlot *slot)
```

## Detailed Description
This function serves as the bridge between index scanning and heap tuple retrieval. After `index_getnext_tid` provides a TID, this function fetches the corresponding heap tuple and performs several important operations:

1. Calls `table_index_fetch_tuple` to retrieve the heap tuple at the specified TID into the provided slot
2. Handles HOT (Heap Only Tuple) chains by potentially following chain links to find visible tuples
3. Updates statistics when a tuple is successfully fetched
4. Manages the kill_prior_tuple flag for dead tuple cleanup optimization (except during recovery)
5. Maintains buffer pins on the heap page until the next tuple fetch or scan end

The function returns true if a visible tuple was found, false otherwise. Multiple calls may be needed to traverse HOT chains, though with MVCC snapshots typically only one matching tuple should exist.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure containing the scan state and most recently fetched TID
- `slot`: TupleTableSlot where the retrieved heap tuple will be stored

## Dependencies
- Functions called/Symbols referenced:
  - `table_index_fetch_tuple` (retrieves heap tuple from TID)
  - `pgstat_count_heap_fetch` (updates heap access statistics)
  - `[IndexScanDesc](../I/IndexScanDesc.md)` (scan descriptor type)
- Called from (representative examples):
  - `[index_getnext_slot](index_getnext_slot.md)` (src/backend/access/index/indexam.c:697)
  - `[IndexOnlyNext](../I/IndexOnlyNext.md)` (src/backend/executor/nodeIndexonlyscan.c:168)
  - `get_actual_variable_endpoint` (src/backend/utils/adt/selfuncs.c:6418)

## Notes and Other Information
- The caller must check `scan->xs_recheck` and perform scan key rechecking if required
- Buffer pins are maintained automatically and will be dropped in subsequent calls
- The kill_prior_tuple optimization is disabled during recovery to maintain MVCC consistency
- HOT chains may require multiple tuple fetches, though MVCC snapshots typically limit this to one visible tuple
- The function handles both successful tuple retrieval and cases where only dead tuples are found
- Location: src/backend/access/index/indexam.c:632-672