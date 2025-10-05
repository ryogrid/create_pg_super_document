# _bt_parallel_build_main

## Location
[src/backend/access/nbtree/nbtsort.c:1740-1861](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L1740-L1861)

## Overview
Entry point function executed by parallel worker processes during B-tree index construction, responsible for setting up the worker environment and coordinating the parallel sorting process.

## Definition

```c
void
_bt_parallel_build_main(dsm_segment *seg, shm_toc *toc)
```
## Detailed Description
This function serves as the main entry point for parallel worker processes during B-tree index builds. It performs the complete workflow for a worker process:

1. **Environment Setup**: Establishes the worker's execution context by setting up debug query strings and reporting activity status
2. **Shared State Access**: Retrieves shared state information from the table of contents (TOC) in the dynamic shared memory segment
3. **Resource Initialization**: Opens required relations (heap and index) with appropriate lock modes and initializes BTSpool structures for sorting
4. **Parallel Coordination**: Attaches to shared tuplesort states and prepares for parallel execution
5. **Work Execution**: Delegates the actual scanning and sorting work to 
6. **Statistics Reporting**: Tracks and reports buffer usage and WAL statistics for the parallel execution
7. **Cleanup**: Releases resources including relation locks

The function handles both unique and non-unique index builds, setting up secondary spools when necessary for unique constraint processing.

## Parameters / Member Variables
- `*seg`: Dynamic shared memory segment containing shared state for the parallel build operation
- `*toc`: Shared memory table of contents for locating various shared data structures within the segment
## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_lookup](../s/shm_toc_lookup.md)
  - [pgstat_report_activity](../p/pgstat_report_activity.md)
  - [table_open](../t/table_open.md)/index_open
  - [tuplesort_attach_shared](../t/tuplesort_attach_shared.md)
  - [InstrStartParallelQuery](../I/InstrStartParallelQuery.md)/InstrEndParallelQuery
  - [_bt_parallel_scan_and_sort](_bt_parallel_scan_and_sort.md)
  - [table_close](../t/table_close.md)/index_close
- Called from (representative examples):
  - Parallel worker processes launched during B-tree index creation

## Notes and Other Information
- Function assumes worker process has been properly initialized with appropriate status flags (PROC_IN_SAFE_IC or none)
- Supports both concurrent and non-concurrent index builds with different locking strategies
- Memory allocation for sorting is divided among worker processes based on maintenance_work_mem
- Includes conditional compilation support for B-tree build statistics collection
- Worker process responsibility ends after calling this function - no further coordination required

## Simplified Source

```c
void
_bt_parallel_build_main(dsm_segment *seg, shm_toc *toc)
{
    BTSpool *btspool, *btspool2;
    BTShared *btshared;
    Sharedsort *sharedsort, *sharedsort2;
    Relation heapRel, indexRel;
    LOCKMODE heapLockmode, indexLockmode;
    WalUsage *walusage;
    BufferUsage *bufferusage;
    int sortmem;

    // Set up debug query string for worker
    char *sharedquery = shm_toc_lookup(toc, PARALLEL_KEY_QUERY_TEXT, true);
    debug_query_string = sharedquery;
    pgstat_report_activity(STATE_RUNNING, debug_query_string);

    // Get shared B-tree state
    btshared = shm_toc_lookup(toc, PARALLEL_KEY_BTREE_SHARED, false);

    // Determine lock modes based on concurrent vs regular build
    if (!btshared->isconcurrent) {
        heapLockmode = ShareLock;
        indexLockmode = AccessExclusiveLock;
    } else {
        heapLockmode = ShareUpdateExclusiveLock;
        indexLockmode = RowExclusiveLock;
    }

    // Open relations
    heapRel = table_open(btshared->heaprelid, heapLockmode);
    indexRel = index_open(btshared->indexrelid, indexLockmode);

    // Initialize primary spool
    btspool = setup_worker_spool(heapRel, indexRel, btshared);

    // Set up tuplesort shared state
    sharedsort = shm_toc_lookup(toc, PARALLEL_KEY_TUPLESORT, false);
    tuplesort_attach_shared(sharedsort, seg);

    // Initialize secondary spool for unique indexes
    if (!btshared->isunique) {
        btspool2 = NULL;
        sharedsort2 = NULL;
    } else {
        btspool2 = setup_secondary_spool(btspool);
        sharedsort2 = shm_toc_lookup(toc, PARALLEL_KEY_TUPLESORT_SPOOL2, false);
        tuplesort_attach_shared(sharedsort2, seg);
    }

    // Start instrumentation tracking
    InstrStartParallelQuery();

    // Perform the actual scanning and sorting work
    sortmem = maintenance_work_mem / btshared->scantuplesortstates;
    _bt_parallel_scan_and_sort(btspool, btspool2, btshared, sharedsort,
                               sharedsort2, sortmem, false);

    // Report usage statistics
    bufferusage = shm_toc_lookup(toc, PARALLEL_KEY_BUFFER_USAGE, false);
    walusage = shm_toc_lookup(toc, PARALLEL_KEY_WAL_USAGE, false);
    InstrEndParallelQuery(&bufferusage[ParallelWorkerNumber],
                          &walusage[ParallelWorkerNumber]);

    // Clean up
    index_close(indexRel, indexLockmode);
    table_close(heapRel, heapLockmode);
}
```