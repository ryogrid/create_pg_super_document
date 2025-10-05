# brininsert

## Location
[src/backend/access/brin/brin.c:335-502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L335-L502)

## Overview
The `brininsert` function handles the insertion of a new tuple into a BRIN index by updating the summary information for the corresponding page range and optionally triggering auto-summarization of previous ranges.

## Definition
```c
bool brininsert(Relation idxRel, Datum *values, bool *nulls,
                ItemPointer heaptid, Relation heapRel,
                IndexUniqueCheck checkUnique,
                bool indexUnchanged,
                IndexInfo *indexInfo)
```

## Detailed Description
This function is called when a tuple is inserted into a heap table that has a BRIN index. BRIN indexes store summary information for ranges of heap pages, so when a new tuple is inserted, the function must:

1. Determine which page range the new tuple belongs to
2. Retrieve the existing BRIN summary tuple for that range (if any)
3. Check if the new tuple values are consistent with the existing summary
4. Update the summary tuple if necessary to include the new tuple values
5. Handle auto-summarization of previous ranges when enabled

The function implements a retry loop to handle concurrent updates, as other processes might be modifying the same BRIN tuple simultaneously. If auto-summarization is enabled, it also requests background summarization of the previous page range when inserting the first tuple of a new range.

## Parameters / Member Variables
- `idxRel`: The BRIN index relation being updated
- `values`: Array of column values from the inserted heap tuple  
- `nulls`: Array indicating which values are null
- `heaptid`: Item pointer (TID) of the inserted heap tuple
- `heapRel`: The heap relation that was inserted into
- `checkUnique`: Uniqueness checking mode (unused for BRIN)
- `indexUnchanged`: Whether the indexed columns changed (unused for BRIN)
- `indexInfo`: Index information structure containing cached state

## Dependencies
- Functions called/Symbols referenced:
  - [initialize_brin_insertstate](../i/initialize_brin_insertstate.md)() (initializes insertion state if needed)
  - `BrinGetAutoSummarize()` (checks if auto-summarization is enabled)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)() (extracts block number from TID)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)() (extracts offset from TID)
  - [brinGetTupleForHeapBlock](brinGetTupleForHeapBlock.md)() (retrieves BRIN tuple for a page range)
  - [AutoVacuumRequestWork](../A/AutoVacuumRequestWork.md)() (requests background summarization)
  - `AllocSetContextCreate()` (creates temporary memory context)
  - [brin_deform_tuple](brin_deform_tuple.md)() (converts BRIN tuple to memory format)
  - [add_values_to_range](../a/add_values_to_range.md)() (updates summary with new values)
  - [brin_copy_tuple](brin_copy_tuple.md)() (creates copy of BRIN tuple)
  - [brin_form_tuple](brin_form_tuple.md)() (converts memory tuple to disk format)
  - [brin_can_do_samepage_update](brin_can_do_samepage_update.md)() (checks if in-place update is possible)
  - [brin_doupdate](brin_doupdate.md)() (performs the actual tuple update)
  - Memory management functions (`MemoryContextSwitchTo()`, `MemoryContextReset()`, etc.)

- Called from (representative examples):
  - PostgreSQL index access method infrastructure (via `brinhandler()`)
  - Tuple insertion operations on tables with BRIN indexes

## Notes and Other Information
- The function always returns `false` as BRIN indexes never enforce uniqueness constraints
- Uses a retry loop to handle concurrent updates to the same BRIN tuple
- Creates a temporary memory context for tuple operations to avoid memory leaks
- Auto-summarization helps keep BRIN indexes up-to-date by summarizing previously unsummarized ranges
- The `pages_per_range` parameter (from the index definition) determines how many heap pages are covered by each BRIN tuple
- If a page range is not yet summarized (no BRIN tuple exists), the insertion is essentially a no-op
- Performance is optimized by caching the `BrinInsertState` across multiple insertions in the same command

## Simplified Source

```c
bool brininsert(Relation idxRel, Datum *values, bool *nulls,
                ItemPointer heaptid, Relation heapRel,
                IndexUniqueCheck checkUnique,
                bool indexUnchanged,
                IndexInfo *indexInfo) {
    // Initialize state for first insertion in this command
    BrinInsertState *bistate = (BrinInsertState *) indexInfo->ii_AmCache;
    if (!bistate)
        bistate = initialize_brin_insertstate(idxRel, indexInfo);

    // Calculate which page range this tuple belongs to
    BlockNumber origHeapBlk = ItemPointerGetBlockNumber(heaptid);
    BlockNumber heapBlk = (origHeapBlk / bistate->bis_pages_per_range) * bistate->bis_pages_per_range;
    bool autosummarize = BrinGetAutoSummarize(idxRel);

    for (;;) {
        // Handle auto-summarization of previous range if needed
        if (autosummarize && heapBlk > 0 && heapBlk == origHeapBlk) {
            // Request summarization of previous range if it's empty
            BlockNumber lastPageRange = heapBlk - 1;
            BrinTuple *lastPageTuple = brinGetTupleForHeapBlock(revmap, lastPageRange, &buf, &off, NULL, BUFFER_LOCK_SHARE);
            if (!lastPageTuple) {
                AutoVacuumRequestWork(AVW_BRINSummarizeRange, RelationGetRelid(idxRel), lastPageRange);
            }
        }

        // Get existing BRIN tuple for this page range
        BrinTuple *brtup = brinGetTupleForHeapBlock(bistate->bis_rmAccess, heapBlk, &buf, &off, NULL, BUFFER_LOCK_SHARE);

        // If range not summarized yet, nothing to do
        if (!brtup)
            break;

        // Convert to memory format and check if update needed
        BrinMemTuple *dtup = brin_deform_tuple(bistate->bis_desc, brtup, NULL);
        bool need_insert = add_values_to_range(idxRel, bistate->bis_desc, dtup, values, nulls);

        if (!need_insert) {
            // Values fit within existing summary - done
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            break;
        } else {
            // Need to update summary tuple
            Size origsz = ItemIdGetLength(PageGetItemId(BufferGetPage(buf), off));
            BrinTuple *origtup = brin_copy_tuple(brtup, origsz, NULL, NULL);

            // Create new tuple and attempt update
            Size newsz;
            BrinTuple *newtup = brin_form_tuple(bistate->bis_desc, heapBlk, dtup, &newsz);
            bool samepage = brin_can_do_samepage_update(buf, origsz, newsz);
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);

            // Try to update - retry on concurrent modification
            if (!brin_doupdate(idxRel, bistate->bis_pages_per_range, bistate->bis_rmAccess,
                               heapBlk, buf, off, origtup, origsz, newtup, newsz, samepage)) {
                continue; // Retry from beginning
            }
            break; // Success
        }
    }

    // Cleanup and return
    if (BufferIsValid(buf))
        ReleaseBuffer(buf);
    return false; // BRIN never enforces uniqueness
}
```