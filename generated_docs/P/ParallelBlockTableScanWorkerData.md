# ParallelBlockTableScanWorkerData

## Location
src/include/access/relscan.h: 90 - 96

## Overview
A structure that maintains per-worker state for parallel table scans in block-oriented storage systems, tracking allocation progress and chunk management for individual worker processes.

## Definition
```c
typedef struct ParallelBlockTableScanWorkerData
{
    uint64      phsw_nallocated;        /* Current # of blocks into the scan */
    uint32      phsw_chunk_remaining;   /* # blocks left in this chunk */
    uint32      phsw_chunk_size;        /* The number of blocks to allocate in
                                         * each I/O chunk for the scan */
} ParallelBlockTableScanWorkerData;
```

## Detailed Description
ParallelBlockTableScanWorkerData represents the per-worker private state for parallel table scans on block-oriented storage. Each worker process in a parallel scan maintains its own instance of this structure to track its current position in the scan, manage I/O chunking for efficient sequential reads, and coordinate with the shared parallel scan state. The structure implements a chunking strategy where workers allocate blocks in batches to optimize I/O patterns and reduce contention on shared state.

## Parameters / Member Variables
- `phsw_nallocated`: uint64 - Current number of blocks the worker has processed into the scan, tracking its progress through the relation
- `phsw_chunk_remaining`: uint32 - Number of blocks remaining in the current I/O chunk that this worker is processing
- `phsw_chunk_size`: uint32 - The size of I/O chunks in blocks that this worker allocates at once to optimize sequential I/O patterns

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references - uses built-in types)
- Called from (representative examples):
  - heap_beginscan
  - HeapScanDescData (as embedded member)
  - ParallelBlockTableScanWorker

## Notes and Other Information
- This structure is specific to each worker process and is not shared between workers
- The chunking mechanism helps reduce mutex contention by allowing workers to operate on multiple blocks before needing to synchronize
- The chunk size can be tuned to balance between I/O efficiency and work distribution fairness among workers
- Part of PostgreSQL's parallel query execution framework for heap table scans
- Works in conjunction with ParallelBlockTableScanDesc for coordinating parallel access