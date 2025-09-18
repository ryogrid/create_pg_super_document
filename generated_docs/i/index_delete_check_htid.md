# index_delete_check_htid

## Location
src/backend/access/heap/heapam.c: 8035 - 8094

## Overview
A helper function for heap_index_delete_tuples that performs corruption checks on heap tuple identifiers (HTIDs) found in index tuples during bulk deletion operations.

## Definition
```c
static inline void index_delete_check_htid(TM_IndexDeleteOp *delstate,
                                          Page page, OffsetNumber maxoff,
                                          ItemPointer htid, TM_IndexStatus *istatus)
```

## Detailed Description
This function validates heap tuple identifiers (HTIDs) extracted from index tuples during bulk index deletion operations, serving as a critical corruption detection mechanism. It performs several integrity checks:

1. **Bounds checking**: Verifies that the HTID's offset number doesn't exceed the maximum valid offset on the heap page
2. **Usage validation**: Ensures the item ID is marked as used (not LP_UNUSED)
3. **HOT tuple detection**: For tuples with storage, checks that they are not heap-only tuples (which should never be directly referenced by index tuples)

This is an ideal location for these checks because the index AM holds a buffer lock on the index page containing the TIDs being examined, eliminating concerns about concurrent VACUUM operations. The function can definitively identify corruption when an HTID points to an LP_UNUSED item or a heap-only tuple - conditions that don't occur during standard index scans.

When corruption is detected, the function reports detailed error messages including the problematic HTID coordinates, index page offset, block number, and index relation name to aid in debugging.

## Parameters / Member Variables
- `delstate`: TM_IndexDeleteOp state containing index relation info and current block number
- `page`: Heap page being examined
- `maxoff`: Maximum valid offset number on the heap page
- `htid`: ItemPointer (heap tuple ID) to validate
- `istatus`: TM_IndexStatus containing the index page offset information

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerGetOffsetNumber
  - ItemPointerGetBlockNumber
  - OffsetNumberIsValid
  - PageGetItemId
  - ItemIdIsUsed
  - ItemIdHasStorage
  - ItemIdIsNormal
  - PageGetItem
  - HeapTupleHeaderIsHeapOnly
  - RelationGetRelationName
- Called from (representative examples):
  - heap_index_delete_tuples

## Notes and Other Information
- This is a static inline helper function designed specifically for heap_index_delete_tuples
- Provides comprehensive index corruption detection during bulk deletion operations
- Reports errors with detailed diagnostic information including exact coordinates and relation names
- The checks are particularly valuable because they occur while holding appropriate locks, ensuring reliable corruption detection
- Detects corruption patterns that are not visible during normal index scans due to timing differences
- Uses ereport(ERROR) to immediately terminate processing when corruption is detected