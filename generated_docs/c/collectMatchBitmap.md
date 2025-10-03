# collectMatchBitmap

## Location
[src/backend/access/gin/ginget.c:121-318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginget.c#L121-L318)

## Overview
This is a central function in GIN index scanning that collects tuple identifiers (TIDs) into a match bitmap for all heap tuples that satisfy the search criteria, supporting multiple scan modes including partial matching, ALL mode, and EVERYTHING mode.

## Definition
```c
static bool collectMatchBitmap(GinBtreeData *btree, GinBtreeStack *stack, GinScanEntry scanEntry, Snapshot snapshot)
```

## Detailed Description
The function implements the core logic for collecting matching TIDs from a GIN index entry tree. It supports three distinct search modes:

1. **Partial-match support**: Scans from the current position until the comparePartialFn indicates completion
2. **SEARCH_MODE_ALL**: Scans from the current position until hitting null items or end of attribute
3. **SEARCH_MODE_EVERYTHING**: Scans from the current position until end of attribute

The function handles both posting lists (stored directly in index tuples) and posting trees (separate B-tree structures for large posting lists). For posting trees, it temporarily unlocks pages to prevent deadlocks with vacuum processes and re-finds the position after scanning. The function maintains predicate locks for proper isolation and updates result count predictions for query optimization.

## Parameters / Member Variables
- `btree`: Pointer to GinBtreeData containing btree state and ginstate information
- `stack`: Pointer to GinBtreeStack representing current scan position in the btree
- `scanEntry`: Pointer to GinScanEntry containing search criteria and result bitmap
- `snapshot`: Snapshot for MVCC consistency and predicate locking

## Dependencies
- Functions called/Symbols referenced:
  - [tbm_create](../t/tbm_create.md) (creates the match bitmap)
  - [moveRightIfItNeeded](../m/moveRightIfItNeeded.md) (page navigation helper)
  - [gintuple_get_attrnum](../g/gintuple_get_attrnum.md), gintuple_get_key (tuple access functions)
  - [scanPostingTree](../s/scanPostingTree.md) (for scanning posting trees)
  - [ginReadTuple](../g/ginReadTuple.md), tbm_add_tuples (for direct posting list processing)
  - [ginCompareEntries](../g/ginCompareEntries.md) (for key comparison during re-find)
  - [datumCopy](../d/datumCopy.md) (for value copying during tree scans)
  - [PredicateLockPage](../P/PredicateLockPage.md) (for predicate locking)
  - Various GIN constants (GIN_CAT_NORM_KEY, GIN_SEARCH_MODE_ALL, etc.)
- Called from:
  - [startScanEntry](../s/startScanEntry.md) (src/backend/access/gin/ginget.c:365)

## Notes and Other Information
- This is a static function, only accessible within the ginget.c file
- Returns `true` when scan is complete, `false` if restart from scratch is necessary
- Handles complex locking scenarios to prevent deadlocks with concurrent vacuum operations
- Implements sophisticated re-find logic after unlocking pages during posting tree scans
- Critical component of GIN index query execution that enables efficient bitmap-based result collection
- Supports both exact and partial matching strategies
- Manages memory allocation for copied datums when scanning posting trees
- Maintains scan position across page boundaries and posting tree traversals

## Simplified Source

```c
static bool collectMatchBitmap(GinBtreeData *btree, GinBtreeStack *stack,
                              GinScanEntry scanEntry, Snapshot snapshot)
{
    OffsetNumber attnum;
    Form_pg_attribute attr;

    // Initialize empty match bitmap
    scanEntry->matchBitmap = tbm_create(work_mem * 1024L, NULL);

    // Early exit for partial match on non-normal keys
    if (scanEntry->isPartialMatch &&
        scanEntry->queryCategory != GIN_CAT_NORM_KEY)
        return true;

    // Get attribute info for this scan key
    attnum = scanEntry->attnum;
    attr = TupleDescAttr(btree->ginstate->origTupdesc, attnum - 1);

    // Apply predicate lock to entry page
    PredicateLockPage(btree->index, BufferGetBlockNumber(stack->buffer), snapshot);

    // Main scan loop through index entries
    for (;;) {
        Page page;
        IndexTuple itup;
        Datum idatum;
        GinNullCategory icategory;

        // Move to next page if needed
        if (moveRightIfItNeeded(btree, stack, snapshot) == false)
            return true;

        // Get current index tuple
        page = BufferGetPage(stack->buffer);
        itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, stack->off));

        // Stop if we've moved to a different attribute
        if (gintuple_get_attrnum(btree->ginstate, itup) != attnum)
            return true;

        idatum = gintuple_get_key(btree->ginstate, itup, &icategory);

        // Check scan stop conditions based on mode
        if (scanEntry->isPartialMatch) {
            // Partial match: stop at nulls or when comparison says we're done
            if (icategory != GIN_CAT_NORM_KEY)
                return true;

            int32 cmp = DatumGetInt32(FunctionCall4Coll(
                &btree->ginstate->comparePartialFn[attnum - 1],
                btree->ginstate->supportCollation[attnum - 1],
                scanEntry->queryKey, idatum,
                UInt16GetDatum(scanEntry->strategy),
                PointerGetDatum(scanEntry->extra_data)));

            if (cmp > 0)
                return true;  // Past our target
            else if (cmp < 0) {
                stack->off++;
                continue;  // Before our target
            }
        } else if (scanEntry->searchMode == GIN_SEARCH_MODE_ALL) {
            // ALL mode: stop at null item placeholders
            if (icategory == GIN_CAT_NULL_ITEM)
                return true;
        }

        // Collect TIDs from this entry
        if (GinIsPostingTree(itup)) {
            // Large posting list stored as separate tree
            BlockNumber rootPostingTree = GinGetPostingTree(itup);

            // Copy datum for re-finding after unlock
            if (icategory == GIN_CAT_NORM_KEY)
                idatum = datumCopy(idatum, attr->attbyval, attr->attlen);

            LockBuffer(stack->buffer, GIN_UNLOCK);
            PredicateLockPage(btree->index, rootPostingTree, snapshot);

            // Scan the entire posting tree
            scanPostingTree(btree->index, scanEntry, rootPostingTree);

            // Re-lock and re-find our position
            LockBuffer(stack->buffer, GIN_SHARE);
            page = BufferGetPage(stack->buffer);

            if (!GinPageIsLeaf(page))
                return false;  // Need to restart scan

            // Re-find our tuple position
            for (;;) {
                if (moveRightIfItNeeded(btree, stack, snapshot) == false)
                    ereport(ERROR, (errmsg("failed to re-find tuple")));

                page = BufferGetPage(stack->buffer);
                itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, stack->off));

                if (gintuple_get_attrnum(btree->ginstate, itup) == attnum) {
                    Datum newDatum;
                    GinNullCategory newCategory;
                    newDatum = gintuple_get_key(btree->ginstate, itup, &newCategory);

                    if (ginCompareEntries(btree->ginstate, attnum,
                                        newDatum, newCategory,
                                        idatum, icategory) == 0)
                        break;  // Found our position
                }
                stack->off++;
            }

            // Free copied datum if needed
            if (icategory == GIN_CAT_NORM_KEY && !attr->attbyval)
                pfree(DatumGetPointer(idatum));
        } else {
            // Small posting list stored directly in tuple
            ItemPointer ipd;
            int nipd;

            ipd = ginReadTuple(btree->ginstate, scanEntry->attnum, itup, &nipd);
            tbm_add_tuples(scanEntry->matchBitmap, ipd, nipd, false);
            scanEntry->predictNumberResult += GinGetNPosting(itup);
            pfree(ipd);
        }

        // Move to next entry
        stack->off++;
    }
}
```