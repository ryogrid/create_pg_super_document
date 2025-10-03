# heapam_relation_copy_for_cluster

## Location
[src/backend/access/heap/heapam_handler.c:686-1005](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L686-L1005)

## Overview
This function performs a comprehensive relation copy operation for CLUSTER command, including tuple rewriting, optional sorting, visibility checking, and progress reporting.

## Definition
static void heapam_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap, Relation OldIndex, bool use_sort, TransactionId OldestXmin, TransactionId *xid_cutoff, MultiXactId *multi_cutoff, double *num_tuples, double *tups_vacuumed, double *tups_recently_dead)

## Detailed Description
heapam_relation_copy_for_cluster is the core function that implements the CLUSTER command for heap relations. It copies tuples from an old heap relation to a new one, optionally using an index for ordering or performing a sort operation. The function handles various tuple visibility states, maintains statistics about processed tuples, and provides progress reporting. It can operate in two modes: index-guided copying (when use_sort is false and an index is provided) or scan-and-sort mode (when use_sort is true). The function also handles transaction visibility using HeapTupleSatisfiesVacuum and manages the heap rewrite process through the rewrite infrastructure.

## Parameters / Member Variables
- `OldHeap`: The source relation being clustered
- `NewHeap`: The destination relation receiving the clustered data
- `OldIndex`: Index to use for ordering (can be NULL)
- `use_sort`: Whether to use sorting instead of index-guided copying
- `OldestXmin`: Transaction ID for visibility determination
- `xid_cutoff`: Pointer to transaction ID cutoff for freezing
- `multi_cutoff`: Pointer to MultiXact ID cutoff for freezing
- `num_tuples`: Pointer to count of live tuples processed
- `tups_vacuumed`: Pointer to count of dead tuples removed
- `tups_recently_dead`: Pointer to count of recently dead tuples

## Dependencies
- Functions called/Symbols referenced:
  - [IsSystemRelation](../I/IsSystemRelation.md)
  - RelationGetTargetBlock
  - [begin_heap_rewrite](../b/begin_heap_rewrite.md)
  - [tuplesort_begin_cluster](../t/tuplesort_begin_cluster.md)
  - [index_beginscan](../i/index_beginscan.md), index_rescan, index_endscan
  - [table_beginscan](../t/table_beginscan.md), table_endscan
  - [table_scan_getnextslot](../t/table_scan_getnextslot.md)
  - [index_getnext_slot](../i/index_getnext_slot.md)
  - [HeapTupleSatisfiesVacuum](../H/HeapTupleSatisfiesVacuum.md)
  - [ExecFetchSlotHeapTuple](../E/ExecFetchSlotHeapTuple.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [rewrite_heap_dead_tuple](../r/rewrite_heap_dead_tuple.md)
  - [reform_and_rewrite_tuple](../r/reform_and_rewrite_tuple.md)
  - [tuplesort_performsort](../t/tuplesort_performsort.md), tuplesort_getheaptuple, tuplesort_end
  - [end_heap_rewrite](../e/end_heap_rewrite.md)
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md), pgstat_progress_update_multi_param
- Constants referenced:
  - HEAPTUPLE_DEAD, HEAPTUPLE_LIVE, HEAPTUPLE_RECENTLY_DEAD
  - HEAPTUPLE_INSERT_IN_PROGRESS, HEAPTUPLE_DELETE_IN_PROGRESS
  - PROGRESS_CLUSTER_* (various progress reporting constants)
  - BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (referenced in heapam_handler.c:2631)

## Notes and Other Information
- This is a static function, only accessible within heapam_handler.c
- Supports both index-guided and sequential scan with sorting modes
- Handles all tuple visibility states including in-progress transactions
- Provides comprehensive progress reporting for long-running operations
- Uses SnapshotAny to see all tuples and applies visibility rules manually
- Manages concurrent transaction warnings for system catalogs
- Properly handles buffer locking around tuple visibility checks
- Integrates with the heap rewrite infrastructure for efficient tuple copying
- The function is a critical component of PostgreSQL's CLUSTER command implementation

## Simplified Source

```c
static void
heapam_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
                                 Relation OldIndex, bool use_sort,
                                 TransactionId OldestXmin,
                                 TransactionId *xid_cutoff,
                                 MultiXactId *multi_cutoff,
                                 double *num_tuples,
                                 double *tups_vacuumed,
                                 double *tups_recently_dead)
{
    RewriteState rwstate;
    IndexScanDesc indexScan = NULL;
    TableScanDesc tableScan = NULL;
    HeapScanDesc heapScan;
    bool is_system_catalog = IsSystemRelation(OldHeap);
    Tuplesortstate *tuplesort = NULL;
    TupleTableSlot *slot;
    int natts = RelationGetDescr(NewHeap)->natts;
    Datum *values = (Datum *) palloc(natts * sizeof(Datum));
    bool *isnull = (bool *) palloc(natts * sizeof(bool));
    BufferHeapTupleTableSlot *hslot;

    // Initialize heap rewrite
    rwstate = begin_heap_rewrite(OldHeap, NewHeap, OldestXmin, *xid_cutoff, *multi_cutoff);

    // Set up sorting if requested
    if (use_sort) {
        tuplesort = tuplesort_begin_cluster(RelationGetDescr(OldHeap), OldIndex,
                                            maintenance_work_mem, NULL, TUPLESORT_NONE);
    }

    // Choose scan method: index scan or sequential scan
    if (OldIndex != NULL && !use_sort) {
        // Index-guided scan
        pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_INDEX_SCAN_HEAP);
        indexScan = index_beginscan(OldHeap, OldIndex, SnapshotAny, 0, 0);
        index_rescan(indexScan, NULL, 0, NULL, 0);
    } else {
        // Sequential scan
        pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_SEQ_SCAN_HEAP);
        tableScan = table_beginscan(OldHeap, SnapshotAny, 0, (ScanKey) NULL);
        heapScan = (HeapScanDesc) tableScan;
        pgstat_progress_update_param(PROGRESS_CLUSTER_TOTAL_HEAP_BLKS, heapScan->rs_nblocks);
    }

    slot = table_slot_create(OldHeap, NULL);
    hslot = (BufferHeapTupleTableSlot *) slot;

    // Main scanning loop
    for (;;) {
        HeapTuple tuple;
        Buffer buf;
        bool isdead;

        CHECK_FOR_INTERRUPTS();

        // Fetch next tuple based on scan method
        if (indexScan != NULL) {
            if (!index_getnext_slot(indexScan, ForwardScanDirection, slot))
                break;
        } else {
            if (!table_scan_getnextslot(tableScan, ForwardScanDirection, slot)) {
                pgstat_progress_update_param(PROGRESS_CLUSTER_HEAP_BLKS_SCANNED, heapScan->rs_nblocks);
                break;
            }
        }

        tuple = ExecFetchSlotHeapTuple(slot, false, NULL);
        buf = hslot->buffer;

        // Check tuple visibility
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        switch (HeapTupleSatisfiesVacuum(tuple, OldestXmin, buf)) {
            case HEAPTUPLE_DEAD:
                isdead = true;
                break;
            case HEAPTUPLE_RECENTLY_DEAD:
                *tups_recently_dead += 1;
                /* fall through */
            case HEAPTUPLE_LIVE:
                isdead = false;
                break;
            case HEAPTUPLE_INSERT_IN_PROGRESS:
                // Handle concurrent inserts
                if (!is_system_catalog &&
                    !TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tuple->t_data)))
                    elog(WARNING, "concurrent insert in progress within table \"%s\"",
                         RelationGetRelationName(OldHeap));
                isdead = false;
                break;
            case HEAPTUPLE_DELETE_IN_PROGRESS:
                // Handle concurrent deletes
                if (!is_system_catalog &&
                    !TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(tuple->t_data)))
                    elog(WARNING, "concurrent delete in progress within table \"%s\"",
                         RelationGetRelationName(OldHeap));
                *tups_recently_dead += 1;
                isdead = false;
                break;
            default:
                elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
                isdead = false;
                break;
        }
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);

        // Handle dead tuples
        if (isdead) {
            *tups_vacuumed += 1;
            if (rewrite_heap_dead_tuple(rwstate, tuple)) {
                *tups_vacuumed += 1;
                *tups_recently_dead -= 1;
            }
            continue;
        }

        // Process live tuples
        *num_tuples += 1;
        if (tuplesort != NULL) {
            // Sort mode: add to sort
            tuplesort_putheaptuple(tuplesort, tuple);
            pgstat_progress_update_param(PROGRESS_CLUSTER_HEAP_TUPLES_SCANNED, *num_tuples);
        } else {
            // Direct mode: rewrite immediately
            reform_and_rewrite_tuple(tuple, OldHeap, NewHeap, values, isnull, rwstate);
            pgstat_progress_update_param(PROGRESS_CLUSTER_HEAP_TUPLES_SCANNED, *num_tuples);
            pgstat_progress_update_param(PROGRESS_CLUSTER_HEAP_TUPLES_WRITTEN, *num_tuples);
        }
    }

    // Clean up scan resources
    if (indexScan != NULL) index_endscan(indexScan);
    if (tableScan != NULL) table_endscan(tableScan);
    if (slot) ExecDropSingleTupleTableSlot(slot);

    // Handle sorted output
    if (tuplesort != NULL) {
        double n_tuples = 0;

        pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_SORT_TUPLES);
        tuplesort_performsort(tuplesort);

        pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_WRITE_NEW_HEAP);

        // Read sorted tuples and rewrite them
        for (;;) {
            HeapTuple tuple = tuplesort_getheaptuple(tuplesort, true);
            if (tuple == NULL) break;

            n_tuples += 1;
            reform_and_rewrite_tuple(tuple, OldHeap, NewHeap, values, isnull, rwstate);
            pgstat_progress_update_param(PROGRESS_CLUSTER_HEAP_TUPLES_WRITTEN, n_tuples);
        }

        tuplesort_end(tuplesort);
    }

    // Finalize rewrite and clean up
    end_heap_rewrite(rwstate);
    pfree(values);
    pfree(isnull);
}
```