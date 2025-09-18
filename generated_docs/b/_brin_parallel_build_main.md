# _brin_parallel_build_main

## Location
src/backend/access/brin/brin.c: 2853 - 2942

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
  - shm_toc_lookup: Retrieves shared structures from the table of contents
  - pgstat_report_activity: Reports worker activity to the statistics collector
  - table_open/index_open: Opens heap and index relations
  - initialize_brin_buildstate: Sets up worker-specific build state
  - tuplesort_attach_shared: Attaches to shared tuplesort state
  - InstrStartParallelQuery/InstrEndParallelQuery: Tracks performance metrics
  - _brin_parallel_scan_and_build: Performs the actual scanning and building work
  - table_close/index_close: Closes relations when work is complete

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