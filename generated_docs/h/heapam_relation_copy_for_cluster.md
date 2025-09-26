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