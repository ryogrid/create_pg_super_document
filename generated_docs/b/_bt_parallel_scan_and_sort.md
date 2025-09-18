# _bt_parallel_scan_and_sort

## Location
[src/backend/access/nbtree/nbtsort.c:1862-1963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L1862-L1963)

## Overview
Core function that performs the actual table scanning and tuple sorting work for a parallel worker during B-tree index construction.

## Definition


## Detailed Description
This function implements the worker's portion of a parallel B-tree index build by performing the following key operations:

1. **Tuplesort Initialization**: Sets up local tuplesort coordination state and begins partial tuplesort operations for the primary spool and optionally a secondary spool for unique indexes
2. **Build State Setup**: Initializes BTBuildState structure with shared configuration and local spool references
3. **Parallel Table Scan**: Joins the parallel table scan initiated by the leader process and processes tuples through the build callback
4. **Sorting Execution**: Performs the actual sorting operation on collected tuples using tuplesort_performsort
5. **Statistics Aggregation**: Updates shared statistics including tuple counts, dead tuple flags, and broken HOT chain detection
6. **Worker Coordination**: Signals completion to the leader process and cleans up local resources

For unique indexes, the function manages two separate tuplesort states - one for live tuples and another for dead tuples, with different memory allocations.

## Parameters / Member Variables
- : Primary BTSpool structure containing heap relation, index relation, and sorting state
- : Secondary BTSpool for dead tuples in unique index builds (NULL for non-unique indexes)
- : Shared state structure containing configuration and coordination data
- : Shared tuplesort state for the primary spool
- : Shared tuplesort state for the secondary spool (NULL if not needed)
- : Amount of working memory allocated to this worker in KB
- : Boolean flag indicating whether to report progress updates

## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_begin_index_btree](../t/tuplesort_begin_index_btree.md)
  - [BuildIndexInfo](../B/BuildIndexInfo.md)
  - [table_beginscan_parallel](../t/table_beginscan_parallel.md)
  - [table_index_build_scan](../t/table_index_build_scan.md)
  - [_bt_build_callback](_bt_build_callback.md)
  - tuplesort_performsort
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - SpinLockAcquire/SpinLockRelease
  - ConditionVariableSignal
  - tuplesort_end
- Called from (representative examples):
  - [_bt_parallel_build_main](_bt_parallel_build_main.md)
  - [_bt_leader_participate_as_worker](_bt_leader_participate_as_worker.md)

## Notes and Other Information
- Function is marked static and only used within the nbtsort.c module
- Memory management for secondary spool is conservative, using work_mem or sortmem (whichever is smaller)
- Progress reporting is conditional and integrated with PostgreSQL's progress tracking infrastructure
- Uses spin locks for thread-safe updates to shared statistics
- Worker completion is signaled through condition variables to minimize leader polling
- Tuplesort states are ended immediately after sorting completion to free resources promptly