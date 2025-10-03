# index_delete_check_htid

## Location
[src/backend/access/heap/heapam.c:8035-8094](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L8035-L8094)

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
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - OffsetNumberIsValid
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsUsed
  - ItemIdHasStorage
  - ItemIdIsNormal
  - [PageGetItem](../P/PageGetItem.md)
  - HeapTupleHeaderIsHeapOnly
  - RelationGetRelationName
- Called from (representative examples):
  - [heap_index_delete_tuples](../h/heap_index_delete_tuples.md)

## Notes and Other Information
- This is a static inline helper function designed specifically for heap_index_delete_tuples
- Provides comprehensive index corruption detection during bulk deletion operations
- Reports errors with detailed diagnostic information including exact coordinates and relation names
- The checks are particularly valuable because they occur while holding appropriate locks, ensuring reliable corruption detection
- Detects corruption patterns that are not visible during normal index scans due to timing differences
- Uses ereport(ERROR) to immediately terminate processing when corruption is detected

## Simplified Source

```c
static inline void index_delete_check_htid(TM_IndexDeleteOp *delstate,
                                          Page page, OffsetNumber maxoff,
                                          ItemPointer htid, TM_IndexStatus *istatus)
{
    OffsetNumber offset = ItemPointerGetOffsetNumber(htid);

    // Check 1: Offset beyond valid range
    if (unlikely(offset > maxoff))
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                       errmsg_internal("heap tid (%u,%u) points past end of page at offset %u in index \"%s\"",
                                     ItemPointerGetBlockNumber(htid), offset,
                                     istatus->idxoffnum, RelationGetRelationName(delstate->irel))));

    // Check 2: Item ID must be in use
    ItemId iid = PageGetItemId(page, offset);
    if (unlikely(!ItemIdIsUsed(iid)))
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                       errmsg_internal("heap tid (%u,%u) points to unused item at offset %u in index \"%s\"",
                                     ItemPointerGetBlockNumber(htid), offset,
                                     istatus->idxoffnum, RelationGetRelationName(delstate->irel))));

    // Check 3: If item has storage, ensure it's not heap-only
    if (ItemIdHasStorage(iid))
    {
        HeapTupleHeader htup = (HeapTupleHeader) PageGetItem(page, iid);
        if (unlikely(HeapTupleHeaderIsHeapOnly(htup)))
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                           errmsg_internal("heap tid (%u,%u) points to heap-only tuple at offset %u in index \"%s\"",
                                         ItemPointerGetBlockNumber(htid), offset,
                                         istatus->idxoffnum, RelationGetRelationName(delstate->irel))));
    }
}
```