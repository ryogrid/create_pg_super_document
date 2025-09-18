# ExecMemoizeInitializeDSM

## Location
src/backend/executor/nodeMemoize.c: 1211 - 1235

## Overview
Initializes Dynamic Shared Memory (DSM) space for collecting and sharing memoize execution statistics across parallel worker processes.

## Definition
```c
void ExecMemoizeInitializeDSM(MemoizeState *node, ParallelContext *pcxt)
```

## Detailed Description
ExecMemoizeInitializeDSM allocates and initializes shared memory space that will be used to collect memoize execution statistics from parallel worker processes. This function is part of PostgreSQL's parallel query execution infrastructure for memoize operations. It allocates a SharedMemoizeInfo structure in the shared memory segment that can accommodate instrumentation data from all worker processes.

The function calculates the required memory size, allocates it from the shared memory table of contents (TOC), initializes the memory to zero, sets up the worker count, and registers the shared memory segment with a unique key based on the plan node ID.

## Parameters / Member Variables
- `node`: Pointer to the MemoizeState structure that will store the reference to the shared memory info
- `pcxt`: Pointer to the ParallelContext structure containing parallel execution context and worker information

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_allocate (allocates memory from shared memory TOC)
  - memset (initializes memory to zero)
  - shm_toc_insert (registers the allocated memory with a key in the TOC)
- Types referenced:
  - MemoizeState (memoize execution state structure)
  - ParallelContext (parallel execution context)
  - SharedMemoizeInfo (shared memory structure for memoize statistics)
  - MemoizeInstrumentation (individual worker instrumentation data)
- Called from:
  - ExecParallelInitializeDSM (main parallel DSM initialization function)

## Notes and Other Information
- Only performs initialization when instrumentation is enabled (node->ss.ps.instrument is true) and parallel workers are present (pcxt->nworkers > 0)
- The allocated memory includes space for the SharedMemoizeInfo header plus one MemoizeInstrumentation structure per worker
- Uses memset to ensure all uninitialized slots contain zeroes for reliable statistics collection
- The shared memory segment is registered using the plan node ID as the key, allowing workers to locate it later
- Sets up the shared_info pointer in the MemoizeState for later access during execution and statistics retrieval