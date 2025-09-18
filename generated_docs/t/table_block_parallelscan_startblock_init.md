# table_block_parallelscan_startblock_init

## Location
src/backend/access/table/tableam.c: 422 - 491

## Overview
Determines and sets the starting block for parallel sequential scans, computing optimal chunk sizes for work distribution among parallel workers.

## Definition


## Detailed Description
This function initializes the starting block and chunk size for parallel sequential scans. It may be called by multiple parallel workers, but ensures the startblock is set only once through careful synchronization. The function performs several key tasks:

1. **Worker State Reset**: Clears the worker-specific state structure
2. **Chunk Size Calculation**: Computes optimal chunk size based on relation size, aiming to split the relation into PARALLEL_SEQSCAN_NCHUNKS chunks but using the next power of 2 for efficiency
3. **Chunk Size Limits**: Ensures chunk size doesn't exceed PARALLEL_SEQSCAN_MAX_CHUNK_SIZE to maintain good synchronous scan performance
4. **Start Block Determination**: Sets the starting block using either:
   - Block 0 for non-synchronized scans
   - A position from the synchronized scan machinery (via ) for synchronized scans

The function uses a retry mechanism when dealing with synchronized scans - it releases the spinlock to call  and retries to avoid holding the lock during potentially slow operations.

## Parameters / Member Variables
- : Relation being scanned in parallel
- : Worker-specific parallel block scan state (reset and configured by this function)
- : Shared parallel block scan descriptor containing coordination state

## Dependencies
- Functions called/Symbols referenced:
  - memset
  - StaticAssertStmt
  - pg_nextpower2_32
  - Max/Min macros
  - SpinLockAcquire
  - SpinLockRelease
  - ss_get_location
  - MaxBlockNumber
  - InvalidBlockNumber
  - PARALLEL_SEQSCAN_NCHUNKS
  - PARALLEL_SEQSCAN_MAX_CHUNK_SIZE
  - ParallelBlockTableScanWorker (struct type)
  - ParallelBlockTableScanDesc (struct type)
- Called from (representative examples):
  - heap_scan_stream_read_next_parallel
  - table_scan_sample_next_tuple

## Notes and Other Information
- Uses a static assertion to ensure BlockNumber width is compatible with pg_nextpower2_32
- The chunk size algorithm creates between PARALLEL_SEQSCAN_NCHUNKS and PARALLEL_SEQSCAN_NCHUNKS/2 actual chunks by using powers of 2
- Thread-safe through spinlock protection, but uses retry pattern to avoid holding locks during expensive operations
- For synchronized scans, integrates with PostgreSQL's synchronized scan machinery to improve buffer cache locality
- The function is designed to be called multiple times safely - only the first call will actually set the start block
- Chunk size calculation balances parallelism (more chunks = better load distribution) with efficiency (larger chunks = less coordination overhead)