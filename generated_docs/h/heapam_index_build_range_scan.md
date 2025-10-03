# heapam_index_build_range_scan

## Location
[src/backend/access/heap/heapam_handler.c:1173-1747](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L1173-L1747)

## Overview
Performs a range scan of a heap relation to build index entries, handling transaction visibility, HOT chains, and parallel scanning during index creation.

## Definition
```c
static double heapam_index_build_range_scan(Relation heapRelation,
                                          Relation indexRelation,
                                          IndexInfo *indexInfo,
                                          bool allow_sync,
                                          bool anyvisible,
                                          bool progress,
                                          BlockNumber start_blockno,
                                          BlockNumber numblocks,
                                          IndexBuildCallback callback,
                                          void *callback_state,
                                          TableScanDesc scan)
```

## Detailed Description
This function is the core implementation for heap table scanning during index builds. It handles complex tuple visibility checking using different snapshot strategies based on the build type (serial vs concurrent, bootstrap vs normal). The function manages HOT (Heap-Only-Tuples) chains by ensuring index entries point to root tuples, preserving chain structure. It supports both serial and parallel index builds, with comprehensive progress reporting and proper handling of transaction isolation levels.

The function performs detailed visibility checks for each tuple, deciding whether to index it based on transaction state (LIVE, DEAD, RECENTLY_DEAD, INSERT_IN_PROGRESS, DELETE_IN_PROGRESS). For concurrent transactions, it may wait for completion to ensure proper uniqueness checking. It extracts index attribute values from tuples and calls the index access method callback to process each qualifying tuple.

## Parameters / Member Variables
- `heapRelation`: The heap table being scanned for index building
- `indexRelation`: The index being constructed
- `indexInfo`: Metadata about the index including uniqueness constraints and predicates
- `allow_sync`: Whether synchronized scanning is permitted for performance
- `anyvisible`: Special mode that considers all visible tuples regardless of transaction state
- `progress`: Whether to report scan progress for monitoring
- `start_blockno`: Starting block number for range scanning
- `numblocks`: Number of blocks to scan (InvalidBlockNumber for all)
- `callback`: Index AM callback function to process each tuple
- `callback_state`: State data passed to the callback function
- `scan`: Optional pre-existing table scan descriptor for parallel builds

## Dependencies
- Functions called/Symbols referenced:
  - [heap_getnext](heap_getnext.md)
  - [heapam_scan_get_blocks_done](heapam_scan_get_blocks_done.md)
  - [HeapTupleSatisfiesVacuum](../H/HeapTupleSatisfiesVacuum.md)
  - [heap_get_root_tuples](heap_get_root_tuples.md)
  - [FormIndexDatum](../F/FormIndexDatum.md)
  - [ExecQual](../E/ExecQual.md)
  - [table_beginscan_strat](../t/table_beginscan_strat.md)
  - [GetOldestNonRemovableTransactionId](../G/GetOldestNonRemovableTransactionId.md)
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md)

## Notes and Other Information
This function implements sophisticated visibility logic to handle various tuple states during index building. It must balance performance (via synchronized scanning) with correctness (proper visibility checking). The HOT chain handling ensures that index entries maintain proper relationships with heap tuples even after updates. The function supports both snapshot-based visibility (for concurrent builds) and custom visibility logic (for regular builds) to maintain MVCC semantics throughout the index creation process.

## Simplified Source

```c
static double heapam_index_build_range_scan(Relation heapRelation, Relation indexRelation,
                                          IndexInfo *indexInfo, bool allow_sync, bool anyvisible,
                                          bool progress, BlockNumber start_blockno, BlockNumber numblocks,
                                          IndexBuildCallback callback, void *callback_state,
                                          TableScanDesc scan) {
    HeapScanDesc hscan;
    bool is_system_catalog = IsSystemRelation(heapRelation);
    bool checking_uniqueness = (indexInfo->ii_Unique || indexInfo->ii_ExclusionOps != NULL);
    HeapTuple heapTuple;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    double reltuples = 0;
    ExprState *predicate;
    TupleTableSlot *slot;
    EState *estate;
    ExprContext *econtext;
    Snapshot snapshot;
    bool need_unregister_snapshot = false;
    TransactionId OldestXmin;
    BlockNumber root_blkno = InvalidBlockNumber;
    OffsetNumber root_offsets[MaxHeapTuplesPerPage];

    // Set up execution environment for expressions and predicates
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);
    slot = table_slot_create(heapRelation, NULL);
    econtext->ecxt_scantuple = slot;
    predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

    // Determine snapshot strategy and transaction visibility threshold
    OldestXmin = InvalidTransactionId;
    if (!IsBootstrapProcessingMode() && !indexInfo->ii_Concurrent)
        OldestXmin = GetOldestNonRemovableTransactionId(heapRelation);

    // Initialize scan if not provided (serial build vs parallel build)
    if (!scan) {
        if (!TransactionIdIsValid(OldestXmin)) {
            snapshot = RegisterSnapshot(GetTransactionSnapshot());
            need_unregister_snapshot = true;
        } else {
            snapshot = SnapshotAny;
        }
        scan = table_beginscan_strat(heapRelation, snapshot, 0, NULL, true, allow_sync);
    } else {
        snapshot = scan->rs_snapshot;
    }

    hscan = (HeapScanDesc) scan;

    // Set scan range limits
    if (!allow_sync)
        heap_setscanlimits(scan, start_blockno, numblocks);

    // Main scan loop - process each tuple
    while ((heapTuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        bool tupleIsAlive;
        CHECK_FOR_INTERRUPTS();

        // Update progress reporting
        if (progress) {
            BlockNumber blocks_done = heapam_scan_get_blocks_done(hscan);
            // Update progress counters...
        }

        // Handle HOT chain root mapping for current page
        if (hscan->rs_cblock != root_blkno) {
            Page page = BufferGetPage(hscan->rs_cbuf);
            LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_SHARE);
            heap_get_root_tuples(page, root_offsets);
            LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);
            root_blkno = hscan->rs_cblock;
        }

        // Determine if tuple should be indexed based on visibility
        if (snapshot == SnapshotAny) {
            bool indexIt = false;

            // Custom visibility checking for different tuple states
            switch (HeapTupleSatisfiesVacuum(heapTuple, OldestXmin, hscan->rs_cbuf)) {
                case HEAPTUPLE_LIVE:
                    indexIt = tupleIsAlive = true;
                    reltuples += 1;
                    break;
                case HEAPTUPLE_DEAD:
                    indexIt = tupleIsAlive = false;
                    break;
                case HEAPTUPLE_RECENTLY_DEAD:
                    indexIt = !HeapTupleIsHotUpdated(heapTuple);
                    tupleIsAlive = false;
                    if (HeapTupleIsHotUpdated(heapTuple))
                        indexInfo->ii_BrokenHotChain = true;
                    break;
                case HEAPTUPLE_INSERT_IN_PROGRESS:
                case HEAPTUPLE_DELETE_IN_PROGRESS:
                    // Handle in-progress transactions with waiting logic
                    indexIt = tupleIsAlive = true;
                    // Wait for conflicting transactions if checking uniqueness...
                    break;
            }

            if (!indexIt) continue;
        } else {
            // MVCC snapshot - heap_getnext already did visibility check
            tupleIsAlive = true;
            reltuples += 1;
        }

        // Evaluate predicate if this is a partial index
        ExecStoreBufferHeapTuple(heapTuple, slot, hscan->rs_cbuf);
        if (predicate != NULL && !ExecQual(predicate, econtext))
            continue;

        // Extract index attribute values from the tuple
        FormIndexDatum(indexInfo, slot, estate, values, isnull);

        // Handle HOT tuples - use root TID instead of heap-only TID
        if (HeapTupleIsHeapOnly(heapTuple)) {
            ItemPointerData tid;
            OffsetNumber offnum = ItemPointerGetOffsetNumber(&heapTuple->t_self);

            // Ensure we have valid root offset mapping
            if (root_offsets[offnum - 1] == InvalidOffsetNumber) {
                // Re-obtain root mappings if needed
                Page page = BufferGetPage(hscan->rs_cbuf);
                LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_SHARE);
                heap_get_root_tuples(page, root_offsets);
                LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);
            }

            ItemPointerSet(&tid, ItemPointerGetBlockNumber(&heapTuple->t_self),
                          root_offsets[offnum - 1]);
            callback(indexRelation, &tid, values, isnull, tupleIsAlive, callback_state);
        } else {
            // Regular tuple - use its own TID
            callback(indexRelation, &heapTuple->t_self, values, isnull, tupleIsAlive, callback_state);
        }
    }

    // Clean up
    table_endscan(scan);
    if (need_unregister_snapshot)
        UnregisterSnapshot(snapshot);
    ExecDropSingleTupleTableSlot(slot);
    FreeExecutorState(estate);

    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_PredicateState = NULL;

    return reltuples;
}
```