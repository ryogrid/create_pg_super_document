# ParallelBlockTableScanWorker

## Location
src/include/access/relscan.h: 97 - 103

## Overview
A typedef for a pointer to ParallelBlockTableScanWorkerData, representing per-worker state management for parallel table scans in block-oriented storage systems.

## Definition
```c
typedef struct ParallelBlockTableScanWorkerData *ParallelBlockTableScanWorker;
```

## Detailed Description
ParallelBlockTableScanWorker is a typedef that defines a pointer type to ParallelBlockTableScanWorkerData structure. This type provides a clean interface for managing individual worker state in parallel table scanning operations. Each worker process in a parallel scan maintains its own instance of the underlying structure to track its progress, manage I/O chunking, and coordinate with the shared parallel scan state. The typedef abstracts the implementation details while providing a consistent API for worker state management across the parallel scanning infrastructure.

## Parameters / Member Variables
This is a typedef, so it doesn't have direct members, but it points to a ParallelBlockTableScanWorkerData structure with these components:
- `phsw_nallocated`: uint64 - Current number of blocks the worker has processed
- `phsw_chunk_remaining`: uint32 - Number of blocks left in current chunk  
- `phsw_chunk_size`: uint32 - Size of I/O chunks for this worker

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelBlockTableScanWorkerData](ParallelBlockTableScanWorkerData.md)
- Called from (representative examples):
  - [table_block_parallelscan_startblock_init](../t/table_block_parallelscan_startblock_init.md)
  - [table_block_parallelscan_nextpage](../t/table_block_parallelscan_nextpage.md)
  - table_scan_sample_next_tuple

## Notes and Other Information
- This typedef is part of PostgreSQL's parallel query execution framework
- Provides per-worker state management separate from shared parallel scan coordination
- Used in conjunction with ParallelBlockTableScanDesc for complete parallel scanning support
- Each worker maintains its own instance to avoid contention and enable independent progress tracking
- The chunking mechanism implemented through this interface helps optimize I/O patterns in parallel scans