# XLogPrefetcherNextBlock

## Location
[src/backend/access/transam/xlogprefetcher.c:461-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L461-L825)

## Overview
A callback function that examines the next block reference in the WAL (Write-Ahead Log) and potentially initiates I/O operations to prefetch blocks that will be needed during replay, making future reads faster.

## Definition

```c
enumber is reused.  It's also more efficient than
				 * discovering that relations don't exist on disk yet with
				 * ENOENT errors.
				 */
				XLogPrefetcherAddFilter(prefetcher, rlocator, 0, record->lsn);
```
## Detailed Description
This function is the core of PostgreSQL's WAL prefetcher system. It operates as a callback within the LSN Read Queue framework, examining upcoming WAL records to identify block references that should be prefetched before they are actually needed during WAL replay.

The function implements a sophisticated analysis of WAL records, handling various edge cases and optimizations:

1. **Record Processing**: Reads ahead in the WAL stream using  when no current record is being processed
2. **Filtering Logic**: Implements intelligent filtering to avoid prefetching blocks that:
   - Don't exist yet (due to creation/truncation operations)
   - Have full page images (FPI) attached
   - Will be initialized from scratch
   - Are duplicates from recent prefetch operations
3. **Operation-Specific Handling**: Special handling for:
   - Database creation with file-copy strategy
   - Relation creation and truncation
   - Timeline changes that require readahead suspension
4. **I/O Management**: Uses  to initiate actual prefetch operations while tracking statistics

## Parameters / Member Variables
- : Opaque pointer that contains the  structure
- : Output parameter that receives the LSN associated with the prefetch operation when I/O is initiated

## Dependencies
- Functions called/Symbols referenced:
  -  - Check if records are queued for replay
  -  - Read future WAL records
  -  - Check if prefetching is enabled
  -  - Check if block should be filtered
  -  - Add block range filters
  -  - Storage manager operations
  -  - Initiate actual prefetch I/O
  -  - Update prefetch statistics
- Called from (representative examples):
  -  - Main prefetcher entry point

## Notes and Other Information
- Returns  when no more WAL data is available
- Returns  when a prefetch I/O operation is initiated
- Returns  when no I/O is needed (cache hit, filtered, etc.)
- Maintains a sliding window of recently prefetched blocks to avoid duplicates
- Implements complex logic to handle database and relation lifecycle events
- Critical for WAL replay performance by reducing I/O wait times during recovery
- Contains extensive debugging support via

## Simplified Source

```c
static LsnReadQueueNextStatus
XLogPrefetcherNextBlock(uintptr_t pgsr_private, XLogRecPtr *lsn)
{
    XLogPrefetcher *prefetcher = (XLogPrefetcher *) pgsr_private;
    XLogReaderState *reader = prefetcher->reader;
    XLogRecPtr replaying_lsn = reader->ReadRecPtr;

    // Main processing loop
    for (;;)
    {
        DecodedXLogRecord *record;

        // Try to read a new future record if needed
        if (prefetcher->record == NULL)
        {
            // Check if we should read ahead or wait
            bool nonblocking = XLogReaderHasQueuedRecordOrError(reader);

            if (nonblocking && replaying_lsn <= prefetcher->no_readahead_until)
                return LRQ_NEXT_AGAIN;

            // Read the next WAL record
            record = XLogReadAhead(prefetcher->reader, nonblocking);
            if (record == NULL)
            {
                if (nonblocking && prefetcher->reader->decode_queue_tail)
                    prefetcher->no_readahead_until = prefetcher->reader->decode_queue_tail->lsn;
                return LRQ_NEXT_AGAIN;
            }

            // Skip if prefetching is disabled
            if (!RecoveryPrefetchEnabled())
            {
                *lsn = InvalidXLogRecPtr;
                return LRQ_NEXT_NO_IO;
            }

            prefetcher->record = record;
            prefetcher->next_block_id = 0;
        }
        else
        {
            record = prefetcher->record;
        }

        // Handle special record types that affect prefetching
        if (replaying_lsn < record->lsn)
        {
            // Check for timeline changes, database creation, relation operations
            if (record->header.xl_rmid == RM_XLOG_ID &&
                (record->header.xl_info & ~XLR_INFO_MASK) == XLOG_CHECKPOINT_SHUTDOWN)
            {
                prefetcher->no_readahead_until = record->lsn;
            }
            // Handle database and storage manager operations with filters
        }

        // Scan block references in the current record
        while (prefetcher->next_block_id <= record->max_block_id)
        {
            int block_id = prefetcher->next_block_id++;
            DecodedBkpBlock *block = &record->blocks[block_id];

            if (!block->in_use)
                continue;

            *lsn = record->lsn;

            // Skip non-main fork blocks
            if (block->forknum != MAIN_FORKNUM)
                return LRQ_NEXT_NO_IO;

            // Skip blocks with full page images or init flags
            if (block->has_image || (block->flags & BKPBLOCK_WILL_INIT))
                return LRQ_NEXT_NO_IO;

            // Skip filtered blocks
            if (XLogPrefetcherIsFiltered(prefetcher, block->rlocator, block->blkno))
                return LRQ_NEXT_NO_IO;

            // Skip recently prefetched blocks
            for (int i = 0; i < XLOGPREFETCHER_SEQ_WINDOW_SIZE; ++i)
            {
                if (block->blkno == prefetcher->recent_block[i] &&
                    RelFileLocatorEquals(block->rlocator, prefetcher->recent_rlocator[i]))
                {
                    return LRQ_NEXT_NO_IO;
                }
            }

            // Update recent block tracking
            prefetcher->recent_rlocator[prefetcher->recent_idx] = block->rlocator;
            prefetcher->recent_block[prefetcher->recent_idx] = block->blkno;
            prefetcher->recent_idx = (prefetcher->recent_idx + 1) % XLOGPREFETCHER_SEQ_WINDOW_SIZE;

            // Check if relation exists and is large enough
            SMgrRelation reln = smgropen(block->rlocator, INVALID_PROC_NUMBER);
            if (!smgrexists(reln, MAIN_FORKNUM) ||
                block->blkno >= smgrnblocks(reln, block->forknum))
            {
                XLogPrefetcherAddFilter(prefetcher, block->rlocator, block->blkno, record->lsn);
                return LRQ_NEXT_NO_IO;
            }

            // Try to initiate prefetch I/O
            PrefetchBufferResult result = PrefetchSharedBuffer(reln, block->forknum, block->blkno);
            if (BufferIsValid(result.recent_buffer))
            {
                // Cache hit
                block->prefetch_buffer = result.recent_buffer;
                return LRQ_NEXT_NO_IO;
            }
            else if (result.initiated_io)
            {
                // I/O started successfully
                block->prefetch_buffer = InvalidBuffer;
                return LRQ_NEXT_IO;
            }
        }

        // Move to next record
        prefetcher->record = NULL;
    }
}
```