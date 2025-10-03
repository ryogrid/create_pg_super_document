# heapam_index_validate_scan

## Location
[src/backend/access/heap/heapam_handler.c:1748-1994](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L1748-L1994)

## Overview
Validates an index by scanning the heap relation and comparing it against existing index entries, inserting any missing tuples that should be indexed.

## Definition
```c
static void heapam_index_validate_scan(Relation heapRelation,
                                     Relation indexRelation,
                                     IndexInfo *indexInfo,
                                     Snapshot snapshot,
                                     ValidateIndexState *state)
```

## Detailed Description
This function performs index validation by conducting a merge-like scan between heap tuples and existing index entries. It scans the heap relation using the provided snapshot and compares each tuple against sorted index entries from a tuplesort to identify missing index entries. When a heap tuple is found that should be indexed but is missing from the index, it inserts the tuple into the index.

The function handles HOT (Heap-Only-Tuples) chains by converting actual tuple TIDs to root TIDs using heap_get_root_tuples mapping. It maintains an in_index array to track which tuples on the current page have already been processed from the tuplesort. The validation process ensures that concurrent index builds or interrupted index builds can be completed by adding any missing entries.

## Parameters / Member Variables
- `heapRelation`: The heap table being validated against the index
- `indexRelation`: The index being validated and potentially updated
- `indexInfo`: Index metadata including uniqueness constraints and predicates
- `snapshot`: Snapshot defining which tuple versions to consider during validation
- `state`: Validation state tracking progress and statistics (htups, tups_inserted)

## Dependencies
- Functions called/Symbols referenced:
  - [heap_getnext](heap_getnext.md)
  - [heap_get_root_tuples](heap_get_root_tuples.md)
  - [tuplesort_getdatum](../t/tuplesort_getdatum.md)
  - [FormIndexDatum](../F/FormIndexDatum.md)
  - [index_insert](../i/index_insert.md)
  - [ExecQual](../E/ExecQual.md)
  - [ItemPointerCompare](../I/ItemPointerCompare.md)
  - [table_beginscan_strat](../t/table_beginscan_strat.md)
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md)

## Notes and Other Information
This function is critical for concurrent index builds and index repair operations. It must handle the complexity of merging heap scan results with sorted index entries while properly managing HOT chains. The function disables synchronized scanning to ensure TIDs are read in the correct order for comparison. The validation process accounts for partial indexes by evaluating predicates and handles uniqueness checking appropriately, even for tuples that might be dead but part of HOT chains.

## Simplified Source

```c
static void heapam_index_validate_scan(Relation heapRelation, Relation indexRelation,
                                     IndexInfo *indexInfo, Snapshot snapshot,
                                     ValidateIndexState *state) {
    TableScanDesc scan;
    HeapScanDesc hscan;
    HeapTuple heapTuple;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    ExprState *predicate;
    TupleTableSlot *slot;
    EState *estate;
    ExprContext *econtext;
    BlockNumber root_blkno = InvalidBlockNumber;
    OffsetNumber root_offsets[MaxHeapTuplesPerPage];
    bool in_index[MaxHeapTuplesPerPage];

    // State variables for merge between heap and index
    ItemPointer indexcursor = NULL;
    ItemPointerData decoded;
    bool tuplesort_empty = false;

    // Set up execution environment for expressions and predicates
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);
    slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation), &TTSOpsHeapTuple);
    econtext->ecxt_scantuple = slot;
    predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

    // Start heap scan - must disable syncscan for ordered TID comparison
    scan = table_beginscan_strat(heapRelation, snapshot, 0, NULL, true, false);
    hscan = (HeapScanDesc) scan;

    // Scan all tuples matching the snapshot
    while ((heapTuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        ItemPointer heapcursor = &heapTuple->t_self;
        ItemPointerData rootTuple;
        OffsetNumber root_offnum;

        CHECK_FOR_INTERRUPTS();
        state->htups += 1;

        // When advancing to new page, build HOT chain root mapping
        if (hscan->rs_cblock != root_blkno) {
            Page page = BufferGetPage(hscan->rs_cbuf);
            LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_SHARE);
            heap_get_root_tuples(page, root_offsets);
            LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);

            // Clear tracking array for new page
            memset(in_index, 0, sizeof(in_index));
            root_blkno = hscan->rs_cblock;
        }

        // Convert heap-only tuple TID to root TID if needed
        rootTuple = *heapcursor;
        root_offnum = ItemPointerGetOffsetNumber(heapcursor);

        if (HeapTupleIsHeapOnly(heapTuple)) {
            root_offnum = root_offsets[root_offnum - 1];
            if (!OffsetNumberIsValid(root_offnum))
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg_internal("failed to find parent tuple for heap-only tuple")));
            ItemPointerSetOffsetNumber(&rootTuple, root_offnum);
        }

        // Merge: advance through index entries until we find or pass current tuple
        while (!tuplesort_empty &&
               (!indexcursor || ItemPointerCompare(indexcursor, &rootTuple) < 0)) {
            Datum ts_val;
            bool ts_isnull;

            // Track index items seen on current heap page
            if (indexcursor && ItemPointerGetBlockNumber(indexcursor) == root_blkno) {
                in_index[ItemPointerGetOffsetNumber(indexcursor) - 1] = true;
            }

            // Get next index entry from tuplesort
            tuplesort_empty = !tuplesort_getdatum(state->tuplesort, true, false,
                                                  &ts_val, &ts_isnull, NULL);
            if (!tuplesort_empty) {
                itemptr_decode(&decoded, DatumGetInt64(ts_val));
                indexcursor = &decoded;
            } else {
                indexcursor = NULL;
            }
        }

        // If index has overshot and we haven't seen this tuple, insert it
        if ((tuplesort_empty || ItemPointerCompare(indexcursor, &rootTuple) > 0) &&
            !in_index[root_offnum - 1]) {

            // Set up tuple for predicate/expression evaluation
            ExecStoreHeapTuple(heapTuple, slot, false);

            // Skip if partial index predicate fails
            if (predicate != NULL && !ExecQual(predicate, econtext))
                continue;

            // Extract index attribute values
            FormIndexDatum(indexInfo, slot, estate, values, isnull);

            // Insert missing tuple into index
            index_insert(indexRelation, values, isnull, &rootTuple, heapRelation,
                        indexInfo->ii_Unique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
                        false, indexInfo);

            state->tups_inserted += 1;
        }
    }

    // Clean up
    table_endscan(scan);
    ExecDropSingleTupleTableSlot(slot);
    FreeExecutorState(estate);

    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_PredicateState = NULL;
}
```