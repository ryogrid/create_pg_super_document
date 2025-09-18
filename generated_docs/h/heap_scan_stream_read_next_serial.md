# heap_scan_stream_read_next_serial

## Location
[src/backend/access/heap/heapam.c:314-337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L314-L337)

## Overview
A streaming read API callback function that provides the next block number for serial sequential scans and TID range scans of heap tables.

## Definition


## Detailed Description
heap_scan_stream_read_next_serial is a callback function used by PostgreSQL's streaming read API to coordinate serial (non-parallel) sequential scans and TID (tuple identifier) range scans of heap tables. Unlike its parallel counterpart, this function handles single-threaded scan operations.

The function operates in two phases:
1. **Initialization phase**: When rs_inited is false, it calls heapgettup_initial_block() to determine the starting block based on scan direction
2. **Subsequent calls**: Uses heapgettup_advance_block() to calculate the next block to read in the specified direction

This function works with both forward and backward scans, and supports TID range scans where only specific ranges of tuple identifiers need to be processed. It returns InvalidBlockNumber when the scan is complete.

## Parameters / Member Variables
- : The ReadStream object managing the streaming read operation
- : Private data passed to the callback, cast to HeapScanDesc containing scan state and parameters
- : Per-buffer data (not used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [heapgettup_initial_block](heapgettup_initial_block.md)
  - [heapgettup_advance_block](heapgettup_advance_block.md)
  - [HeapScanDesc](../H/HeapScanDesc.md) (type cast)
- Called from (representative examples):
  - [heap_beginscan](heap_beginscan.md) (src/backend/access/heap/heapam.c:1180)

## Notes and Other Information
- This function handles both sequential scans and TID range scans in serial (non-parallel) mode
- Unlike the parallel version, this doesn't need coordination between multiple workers
- Uses heapgettup helper functions that handle the logic for determining scan boundaries and advancement
- The rs_prefetch_block field tracks the current block being processed
- Supports both forward and backward scan directions through the heapgettup_advance_block function
- Part of PostgreSQL's streaming read API that optimizes I/O patterns for table scanning
- Returns InvalidBlockNumber to signal scan completion to the streaming read infrastructure