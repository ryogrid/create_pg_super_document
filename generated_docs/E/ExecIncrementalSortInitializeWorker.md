# ExecIncrementalSortInitializeWorker

## Location
src/backend/executor/nodeIncrementalSort.c: 1219 - 1232

## Overview
Initializes a parallel worker process for incremental sort operations by attaching it to shared memory space containing sort statistics.

## Definition


## Detailed Description
This function is part of PostgreSQL's parallel query execution infrastructure, specifically for incremental sort operations. It prepares a worker process to participate in parallel incremental sorting by:

1. Looking up the shared memory segment containing incremental sort statistics using the plan node ID
2. Setting the worker flag to indicate this process is operating as a parallel worker

The function uses the shared memory table of contents (TOC) to locate the SharedIncrementalSortInfo structure that was previously allocated by the leader process. This shared structure contains performance statistics and coordination information that all parallel workers need to access during the incremental sort operation.

## Parameters / Member Variables
- : Pointer to the IncrementalSortState structure representing the current incremental sort execution state
- : Pointer to the ParallelWorkerContext containing the shared memory table of contents and other parallel execution context

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_lookup
- Called from (representative examples):
  - ExecParallelInitializeWorker (src/backend/executor/execParallel.c:1364)

## Notes and Other Information
- This function is only called in parallel query execution contexts where multiple worker processes cooperate on incremental sorting
- The shared_info pointer established here will be used throughout the incremental sort operation to coordinate statistics and state between parallel workers
- The am_worker flag is set to distinguish worker processes from the leader process, affecting how certain operations are handled during execution