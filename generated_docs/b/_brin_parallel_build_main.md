# _brin_parallel_build_main

## Location
[src/backend/access/brin/brin.c:2853-2942](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L2853-L2942)

## Overview
This function serves as the main entry point for parallel worker processes during BRIN index construction, handling worker initialization, relation opening, and coordination with the parallel build infrastructure.

## Definition
```c
void _brin_parallel_build_main(dsm_segment *seg, shm_toc *toc)
```

## Detailed Description
This function is executed by each parallel worker process launched during a parallel BRIN index build. It performs the complete worker initialization sequence: setting up the query context, looking up shared state from the table of contents, opening the required relations with appropriate lock modes, initializing the build state, and attaching to shared sort structures. The function also handles performance instrumentation by tracking buffer and WAL usage during the parallel execution. After setup, it delegates the actual scanning and building work to _brin_parallel_scan_and_build.

## Parameters / Member Variables
- `seg`: The dynamic shared memory segment containing shared state
- `toc`: The shared memory table of contents for locating shared structures

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_lookup](../s/shm_toc_lookup.md): Retrieves shared structures from the table of contents
  - [pgstat_report_activity](../p/pgstat_report_activity.md): Reports worker activity to the statistics collector
  - [table_open](../t/table_open.md)/index_open: Opens heap and index relations
  - [initialize_brin_buildstate](../i/initialize_brin_buildstate.md): Sets up worker-specific build state
  - [tuplesort_attach_shared](../t/tuplesort_attach_shared.md): Attaches to shared tuplesort state
  - [InstrStartParallelQuery](../I/InstrStartParallelQuery.md)/InstrEndParallelQuery: Tracks performance metrics
  - [_brin_parallel_scan_and_build](_brin_parallel_scan_and_build.md): Performs the actual scanning and building work
  - [table_close](../t/table_close.md)/index_close: Closes relations when work is complete

- Called from (representative examples):
  - PostgreSQL parallel worker infrastructure (referenced in brin.h)

## Notes and Other Information
- This is a public function (not static), accessible from other modules
- Workers use different lock modes depending on whether the build is concurrent (ShareUpdateExclusiveLock/RowExclusiveLock) or not (ShareLock/AccessExclusiveLock)
- The function validates that workers only have expected status flags (PROC_IN_SAFE_IC or none)
- Memory allocation (sortmem) is calculated based on maintenance_work_mem divided by the number of sort states
- Workers attach to shared sort state using tuplesort_attach_shared for coordination
- Performance instrumentation tracks both buffer usage and WAL usage per worker
- The function sets debug_query_string and reports activity to help with debugging and monitoring
- Workers receive false for the progress parameter when calling _brin_parallel_scan_and_build (only leader reports progress)
- Each worker gets its own initialized build state but shares coordination structures

## Simplified Source

```c
void _brin_parallel_build_main(dsm_segment *seg, shm_toc *toc) {
    BrinShared *brinshared;
    Sharedsort *sharedsort;
    BrinBuildState *buildstate;
    Relation heapRel, indexRel;
    LOCKMODE heapLockmode, indexLockmode;

    // Setup worker context and activity reporting
    char *sharedquery = shm_toc_lookup(toc, PARALLEL_KEY_QUERY_TEXT, true);
    debug_query_string = sharedquery;
    pgstat_report_activity(STATE_RUNNING, debug_query_string);

    // Look up shared state
    brinshared = shm_toc_lookup(toc, PARALLEL_KEY_BRIN_SHARED, false);

    // Determine lock modes based on build type
    if (!brinshared->isconcurrent) {
        heapLockmode = ShareLock;
        indexLockmode = AccessExclusiveLock;
    } else {
        heapLockmode = ShareUpdateExclusiveLock;
        indexLockmode = RowExclusiveLock;
    }

    // Open relations and initialize build state
    heapRel = table_open(brinshared->heaprelid, heapLockmode);
    indexRel = index_open(brinshared->indexrelid, indexLockmode);
    buildstate = initialize_brin_buildstate(indexRel, NULL,
                                          brinshared->pagesPerRange,
                                          InvalidBlockNumber);

    // Attach to shared sort state
    sharedsort = shm_toc_lookup(toc, PARALLEL_KEY_TUPLESORT, false);
    tuplesort_attach_shared(sharedsort, seg);

    // Start performance tracking
    InstrStartParallelQuery();

    // Calculate memory allocation and do the work
    int sortmem = maintenance_work_mem / brinshared->scantuplesortstates;
    _brin_parallel_scan_and_build(buildstate, brinshared, sharedsort,
                                 heapRel, indexRel, sortmem, false);

    // Report performance metrics
    BufferUsage *bufferusage = shm_toc_lookup(toc, PARALLEL_KEY_BUFFER_USAGE, false);
    WalUsage *walusage = shm_toc_lookup(toc, PARALLEL_KEY_WAL_USAGE, false);
    InstrEndParallelQuery(&bufferusage[ParallelWorkerNumber],
                         &walusage[ParallelWorkerNumber]);

    // Cleanup
    index_close(indexRel, indexLockmode);
    table_close(heapRel, heapLockmode);
}
```