# table_block_parallelscan_nextpage

## Location
src/backend/access/table/tableam.c: 492 - 616

## Overview
Returns the next block number for a parallel table scan worker to process, implementing an optimized chunking strategy to improve sequential I/O performance across multiple parallel workers.

## Definition
```c
BlockNumber table_block_parallelscan_nextpage(Relation rel,
                                              ParallelBlockTableScanWorker pbscanwork,
                                              ParallelBlockTableScanDesc pbscan)
```

## Detailed Description
This function coordinates block allocation among parallel table scan workers by assigning consecutive ranges of blocks ("chunks") to each worker. The chunking strategy optimizes I/O performance by enabling operating systems to detect sequential access patterns and perform effective readahead, even across different backend processes.

The function implements a dynamic chunk size adjustment mechanism that starts with larger chunks and reduces them toward the end of the scan to ensure balanced work distribution. When fewer than PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS chunks remain, the chunk size is halved repeatedly until reaching a minimum size of 1 block.

The allocation uses atomic operations to track globally allocated blocks (phs_nallocated) while maintaining per-worker state for chunk management. Block numbers wrap around using modulo arithmetic to handle circular scanning patterns.

## Parameters / Member Variables
- `rel`: The relation being scanned
- `pbscanwork`: Per-worker local state tracking chunk information and allocation progress
- `pbscan`: Shared parallel scan descriptor containing global scan state and coordination data

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_fetch_add_u64](../p/pg_atomic_fetch_add_u64.md)
  - [ss_report_location](../s/ss_report_location.md)
  - PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS
- Called from (representative examples):
  - [heap_scan_stream_read_next_parallel](../h/heap_scan_stream_read_next_parallel.md)
  - table_scan_sample_next_tuple

## Notes and Other Information
- Returns InvalidBlockNumber when all blocks have been allocated to workers
- Implements sophisticated load balancing by reducing chunk sizes near scan completion
- Coordinates with synchronized scanning (syncscan) feature to report scan progress
- The 64-bit nallocated counter prevents overflow when worker threads increment beyond the actual block count
- Chunk size reduction occurs when remaining work falls below PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS thresholds