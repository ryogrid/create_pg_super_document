# table_block_parallelscan_initialize

## Location
src/backend/access/table/tableam.c: 389 - 406

## Overview
Initializes the shared parallel scan descriptor for block-oriented access methods, setting up coordination state for multiple worker processes to scan a relation in parallel.

## Definition


## Detailed Description
This function initializes a parallel scan descriptor for block-oriented access methods. It sets up the shared state needed to coordinate multiple worker processes scanning the same relation concurrently. The function populates various fields in the  structure that control how blocks are allocated to different worker processes.

Key initialization tasks performed:
1. Records the relation's OID for identification
2. Determines total number of blocks in the relation
3. Decides whether to use synchronized scanning based on relation size and buffer configuration
4. Initializes the mutex for coordinating block allocation between workers
5. Sets the starting block to invalid (to be determined later)
6. Initializes the atomic counter tracking allocated blocks

The synchronized scanning decision follows similar logic to sequential scans - it's enabled for larger relations that don't use local buffers to improve cache locality.

## Parameters / Member Variables
- : Relation to be scanned in parallel
- : Generic parallel table scan descriptor (cast to ParallelBlockTableScanDesc internally)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetRelid
  - RelationGetNumberOfBlocks
  - RelationUsesLocalBuffers
  - SpinLockInit
  - [pg_atomic_init_u64](../p/pg_atomic_init_u64.md)
  - [ParallelBlockTableScanDesc](../P/ParallelBlockTableScanDesc.md) (struct type)
  - [ParallelBlockTableScanDescData](../P/ParallelBlockTableScanDescData.md)
  - InvalidBlockNumber
  - synchronize_seqscans (global variable)
  - NBuffers (global variable)
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md)
  - table_scan_sample_next_tuple

## Notes and Other Information
- Part of the parallel scan infrastructure for block-oriented access methods
- The synchronize_seqscans decision uses the same logic as regular sequential scans
- Relations using local buffers (typically temporary tables) don't use synchronized scanning
- The threshold for synchronized scanning is NBuffers/4, meaning sync scan is used for relations larger than 1/4 of the buffer pool
- Returns the size of the descriptor structure, consistent with the estimate function
- The phs_startblock is set to InvalidBlockNumber initially and gets set by the startblock_init function
- Uses atomic operations for the allocated blocks counter to ensure thread-safety across parallel workers