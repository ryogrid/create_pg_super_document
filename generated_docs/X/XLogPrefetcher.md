# XLogPrefetcher

## Location
[src/backend/access/transam/xlogprefetcher.c:124-159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L124-L159)

## Overview
XLogPrefetcher is a mechanism that wraps an XLogReader to prefetch blocks that will soon be referenced during WAL replay, helping to avoid IO stalls and improve recovery performance.

## Definition

```c
struct XLogPrefetcher
{
	/* WAL reader and current reading state. */
	XLogReaderState *reader;
	DecodedXLogRecord *record;
	int			next_block_id;

	/* When to publish stats. */
	XLogRecPtr	next_stats_shm_lsn;

	/* Book-keeping to avoid accessing blocks that don't exist yet. */
	HTAB	   *filter_table;
	dlist_head	filter_queue;

	/* Book-keeping to avoid repeat prefetches. */
	RelFileLocator recent_rlocator[XLOGPREFETCHER_SEQ_WINDOW_SIZE];
	BlockNumber recent_block[XLOGPREFETCHER_SEQ_WINDOW_SIZE];
	int			recent_idx;

	/* Book-keeping to disable prefetching temporarily. */
	XLogRecPtr	no_readahead_until;

	/* IO depth manager. */
	LsnReadQueue *streaming_read;

	XLogRecPtr	begin_ptr;

	int			reconfigure_count;
};
```
## Detailed Description
The XLogPrefetcher serves as an intelligent WAL prefetching system that wraps around an XLogReaderState to predict and pre-load database blocks that will be needed during WAL replay. It maintains sophisticated book-keeping mechanisms to avoid duplicate prefetches, manage IO depth, filter out blocks that don't exist yet, and temporarily disable prefetching when appropriate. The prefetcher uses a combination of hash tables, circular buffers, and queues to efficiently manage prefetch operations and track performance statistics.

## Parameters / Member Variables
- `*reader`: Pointer to XLogReaderState for reading WAL records
- `*record`: Current decoded WAL record being processed
- `next_block_id`: Index of the next block to prefetch within current record
- `next_stats_shm_lsn`: LSN position when to next publish statistics to shared memory
- `*filter_table`: Hash table (HTAB) for tracking blocks that should be filtered/avoided
- `filter_queue`: Double-linked list queue for managing filter entries in order
- `recent_rlocator[XLOGPREFETCHER_SEQ_WINDOW_SIZE]`: Circular buffer of recently prefetched file locators (size XLOGPREFETCHER_SEQ_WINDOW_SIZE=4)
- `recent_block[XLOGPREFETCHER_SEQ_WINDOW_SIZE]`: Circular buffer of recently prefetched block numbers (size XLOGPREFETCHER_SEQ_WINDOW_SIZE=4)
- `recent_idx`: Current index in the recent_* circular buffers
- `no_readahead_until`: LSN position until which prefetching should be disabled
- `*streaming_read`: LsnReadQueue for managing IO depth and prefetch operations
- `begin_ptr`: Starting LSN position for prefetching
- `reconfigure_count`: Counter for tracking prefetcher reconfiguration events

## Dependencies
- Functions called/Symbols referenced:
  - [XLogReaderState](XLogReaderState.md) (WAL reader state)
  - [DecodedXLogRecord](../D/DecodedXLogRecord.md) (decoded WAL record structure)
  - [HTAB](../H/HTAB.md) (hash table type)
  - [dlist_head](../d/dlist_head.md) (double-linked list head)
  - [RelFileLocator](../R/RelFileLocator.md) (file location identifier)
  - BlockNumber (block number type)
  - XLogRecPtr (WAL position type)
  - [LsnReadQueue](../L/LsnReadQueue.md) (IO depth management queue)
  - XLOGPREFETCHER_SEQ_WINDOW_SIZE (constant defining recent buffer size)

- Called from (representative examples):
  - [XLogPrefetcherAllocate](XLogPrefetcherAllocate.md) (creates and initializes prefetcher)
  - [XLogPrefetcherFree](XLogPrefetcherFree.md) (deallocates prefetcher)
  - [XLogPrefetcherGetReader](XLogPrefetcherGetReader.md) (gets associated reader)
  - [XLogPrefetcherComputeStats](XLogPrefetcherComputeStats.md) (computes prefetch statistics)
  - [XLogPrefetcherNextBlock](XLogPrefetcherNextBlock.md) (gets next block to prefetch)
  - [XLogPrefetcherBeginRead](XLogPrefetcherBeginRead.md) (begins reading with prefetching)
  - [XLogPrefetcherReadRecord](XLogPrefetcherReadRecord.md) (reads record with prefetching)
  - Recovery functions in xlogrecovery.c

## Notes and Other Information
The prefetcher uses a multi-layered approach to optimization: it maintains a small window (size 4) of recently accessed blocks to avoid redundant prefetches, uses a filter mechanism to avoid accessing non-existent blocks, implements temporary prefetch disabling for certain conditions, and manages IO depth through LsnReadQueue. The system is designed to improve WAL replay performance during recovery by predicting which blocks will be needed and loading them asynchronously ahead of time. The prefetcher can be dynamically reconfigured and publishes performance statistics to shared memory for monitoring.