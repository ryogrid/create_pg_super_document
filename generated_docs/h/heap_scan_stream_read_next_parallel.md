# heap_scan_stream_read_next_parallel

## Location
[src/backend/access/heap/heapam.c:276-313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L276-L313)

## Overview
A streaming read API callback function that provides the next block number for parallel sequential heap table scans.

## Definition

```c
static BlockNumber
heap_scan_stream_read_next_parallel(ReadStream *stream,
									void *callback_private_data,
									void *per_buffer_data)
```
## Detailed Description
heap_scan_stream_read_next_parallel is a callback function used by PostgreSQL's streaming read API to coordinate parallel sequential scans of heap tables. It determines which block should be read next by a worker process in a parallel scan operation.

The function works in two phases:
1. **Initialization phase**: When rs_inited is false, it initializes the parallel scan using table_block_parallelscan_startblock_init() and gets the first block to process
2. **Subsequent calls**: Returns the next available block for this worker to process

The function uses PostgreSQL's parallel scan infrastructure to ensure that multiple worker processes don't read the same blocks, providing efficient work distribution across parallel workers. It returns InvalidBlockNumber when no more blocks are available for this worker to process.

## Parameters / Member Variables
- : The ReadStream object managing the streaming read operation
- : Private data passed to the callback, cast to HeapScanDesc containing scan state
- : Per-buffer data (not used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsForward
  - [table_block_parallelscan_startblock_init](../t/table_block_parallelscan_startblock_init.md)
  - [table_block_parallelscan_nextpage](../t/table_block_parallelscan_nextpage.md)
  - [ParallelBlockTableScanDesc](../P/ParallelBlockTableScanDesc.md) (type cast)
  - [HeapScanDesc](../H/HeapScanDesc.md) (type cast)
- Called from (representative examples):
  - [heap_beginscan](heap_beginscan.md) (src/backend/access/heap/heapam.c:1178)

## Notes and Other Information
- This function only works with forward sequential scans (asserted via ScanDirectionIsForward)
- It requires that scan->rs_base.rs_parallel is set, indicating this is a parallel scan
- The function handles both initialization and subsequent block retrieval in a single callback
- Returns InvalidBlockNumber when the worker has no more blocks to process
- Part of PostgreSQL's streaming read API introduced for more efficient parallel scanning
- The rs_prefetch_block field is updated with each call to track the current block being processed