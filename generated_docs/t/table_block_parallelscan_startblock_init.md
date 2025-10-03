# table_block_parallelscan_startblock_init

## Location
[src/backend/access/table/tableam.c:422-491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/tableam.c#L422-L491)

## Overview
Determines and sets the starting block for parallel sequential scans, computing optimal chunk sizes for work distribution among parallel workers.

## Definition

```c
void
table_block_parallelscan_startblock_init(Relation rel,
										 ParallelBlockTableScanWorker pbscanwork,
										 ParallelBlockTableScanDesc pbscan)
```
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
- `rel`: Relation being scanned in parallel
- `pbscanwork`: Worker-specific parallel block scan state (reset and configured by this function)
- `pbscan`: Shared parallel block scan descriptor containing coordination state
## Dependencies
- Functions called/Symbols referenced:
  - memset
  - StaticAssertStmt
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md)
  - Max/Min macros
  - SpinLockAcquire
  - SpinLockRelease
  - [ss_get_location](../s/ss_get_location.md)
  - MaxBlockNumber
  - InvalidBlockNumber
  - PARALLEL_SEQSCAN_NCHUNKS
  - PARALLEL_SEQSCAN_MAX_CHUNK_SIZE
  - [ParallelBlockTableScanWorker](../P/ParallelBlockTableScanWorker.md) (struct type)
  - [ParallelBlockTableScanDesc](../P/ParallelBlockTableScanDesc.md) (struct type)
- Called from (representative examples):
  - [heap_scan_stream_read_next_parallel](../h/heap_scan_stream_read_next_parallel.md)
  - [table_scan_sample_next_tuple](table_scan_sample_next_tuple.md)

## Notes and Other Information
- Uses a static assertion to ensure BlockNumber width is compatible with pg_nextpower2_32
- The chunk size algorithm creates between PARALLEL_SEQSCAN_NCHUNKS and PARALLEL_SEQSCAN_NCHUNKS/2 actual chunks by using powers of 2
- Thread-safe through spinlock protection, but uses retry pattern to avoid holding locks during expensive operations
- For synchronized scans, integrates with PostgreSQL's synchronized scan machinery to improve buffer cache locality
- The function is designed to be called multiple times safely - only the first call will actually set the start block
- Chunk size calculation balances parallelism (more chunks = better load distribution) with efficiency (larger chunks = less coordination overhead)

## Simplified Source

```c
void
table_block_parallelscan_startblock_init(Relation rel,
                                        ParallelBlockTableScanWorker pbscanwork,
                                        ParallelBlockTableScanDesc pbscan)
{
    BlockNumber sync_startpage = InvalidBlockNumber;

    // Reset worker state
    memset(pbscanwork, 0, sizeof(*pbscanwork));

    // Calculate optimal chunk size (power of 2 for efficiency)
    pbscanwork->phsw_chunk_size = pg_nextpower2_32(Max(pbscan->phs_nblocks /
                                                       PARALLEL_SEQSCAN_NCHUNKS, 1));

    // Cap chunk size to prevent performance issues
    pbscanwork->phsw_chunk_size = Min(pbscanwork->phsw_chunk_size,
                                      PARALLEL_SEQSCAN_MAX_CHUNK_SIZE);

retry:
    SpinLockAcquire(&pbscan->phs_mutex);

    // Set start block if not already initialized
    if (pbscan->phs_startblock == InvalidBlockNumber) {
        if (!pbscan->base.phs_syncscan)
            pbscan->phs_startblock = 0;  // Start at beginning for non-sync scans
        else if (sync_startpage != InvalidBlockNumber)
            pbscan->phs_startblock = sync_startpage;  // Use cached sync position
        else {
            // Get sync position (requires releasing lock)
            SpinLockRelease(&pbscan->phs_mutex);
            sync_startpage = ss_get_location(rel, pbscan->phs_nblocks);
            goto retry;
        }
    }
    SpinLockRelease(&pbscan->phs_mutex);
}
```