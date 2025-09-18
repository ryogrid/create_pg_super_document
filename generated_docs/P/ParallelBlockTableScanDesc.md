# ParallelBlockTableScanDesc

## Location
src/include/access/relscan.h: 85 - 89

## Overview
A typedef for a pointer to ParallelBlockTableScanDescData, representing shared state for parallel table scans in block-oriented storage systems.

## Definition


## Detailed Description
ParallelBlockTableScanDesc is a typedef that defines a pointer type to ParallelBlockTableScanDescData structure. This type is used throughout PostgreSQL's parallel table scanning infrastructure to manage shared state between multiple worker processes when scanning block-oriented storage (primarily heap tables). The typedef provides a clean interface for passing around references to the shared parallel scan state without exposing the internal structure details at the interface level.

## Parameters / Member Variables
This is a typedef, so it doesn't have direct members, but it points to a ParallelBlockTableScanDescData structure with these key components:
- : ParallelTableScanDescData - Base parallel scan descriptor data
- : BlockNumber - Number of blocks in relation at start of scan
- : slock_t - Mutual exclusion for setting startblock
- : BlockNumber - Starting block number for the scan
- : pg_atomic_uint64 - Number of blocks allocated to workers so far

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelBlockTableScanDescData](ParallelBlockTableScanDescData.md)
- Called from (representative examples):
  - [heap_scan_stream_read_next_parallel](../h/heap_scan_stream_read_next_parallel.md)
  - [initscan](../i/initscan.md)
  - [heapam_index_build_range_scan](../h/heapam_index_build_range_scan.md)
  - [table_block_parallelscan_initialize](../t/table_block_parallelscan_initialize.md)
  - [table_block_parallelscan_nextpage](../t/table_block_parallelscan_nextpage.md)

## Notes and Other Information
- This typedef is part of PostgreSQL's parallel query execution framework
- Used specifically for block-oriented storage access methods like heap tables
- The underlying structure contains atomic variables and mutexes for thread-safe coordination between parallel workers
- Essential for implementing efficient parallel table scans that avoid work duplication between processes