# PrefetchSharedBuffer

## Location
[src/backend/storage/buffer/bufmgr.c:548-637](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L548-L637)

## Overview
Implements buffer prefetching for shared buffers by checking if a block is already cached and initiating asynchronous I/O if needed.

## Definition

```c
PrefetchBufferResult
PrefetchSharedBuffer(SMgrRelation smgr_reln,
					 ForkNumber forkNum,
					 BlockNumber blockNum)
```
## Detailed Description
PrefetchSharedBuffer is the core implementation function for prefetching blocks in PostgreSQL's shared buffer pool. It first checks if the requested block is already present in the buffer cache by performing a buffer table lookup. If the block is not cached, it attempts to initiate asynchronous I/O using the storage manager's prefetch capability (when USE_PREFETCH is defined and direct I/O is not enabled). The function is designed to optimize future ReadBuffer operations by ensuring data is available when needed, without blocking the current operation.

The function follows a non-blocking approach - it only performs a shared lock on the buffer partition during the lookup phase and does not pin buffers or modify usage counts. This design prevents interference with normal buffer management while providing performance benefits for sequential access patterns.

## Parameters / Member Variables
- `smgr_reln`: Storage manager relation handle for the target relation
- `forkNum`: Fork number identifying which fork of the relation to prefetch from
- `blockNum`: Block number within the fork to prefetch
## Dependencies
- Functions called/Symbols referenced:
  - [InitBufferTag](../I/InitBufferTag.md): Creates buffer tag for block identification
  - [BufTableHashCode](../B/BufTableHashCode.md): Computes hash for buffer table lookup
  - [BufMappingPartitionLock](../B/BufMappingPartitionLock.md): Gets partition lock for buffer mapping
  - [BufTableLookup](../B/BufTableLookup.md): Looks up buffer in buffer table
  - [smgrprefetch](../s/smgrprefetch.md): Initiates asynchronous I/O through storage manager
- Called from (representative examples):
  - [XLogPrefetcherNextBlock](../X/XLogPrefetcherNextBlock.md): WAL replay prefetching
  - [PrefetchBuffer](PrefetchBuffer.md): High-level prefetch interface

## Notes and Other Information
- When a block is found in the buffer cache, the function returns the buffer ID but does not pin it, requiring the caller to revalidate
- The function includes extensive comments about the trade-offs of not bumping usage_count for cached blocks
- Prefetching is disabled when direct I/O is enabled (IO_DIRECT_DATA flag)
- In recovery mode, missing relation files do not cause errors but simply skip prefetching
- The function is conditional on USE_PREFETCH compilation flag for platform compatibility

## Simplified Source
```c
PrefetchBufferResult PrefetchSharedBuffer(SMgrRelation smgr_reln,
                                        ForkNumber forkNum,
                                        BlockNumber blockNum) {
    PrefetchBufferResult result = {InvalidBuffer, false};
    BufferTag newTag;
    uint32 newHash;
    LWLock *newPartitionLock;
    int buf_id;

    // Create buffer tag for the requested block
    InitBufferTag(&newTag, &smgr_reln->smgr_rlocator.locator, forkNum, blockNum);

    // Calculate hash and get partition lock
    newHash = BufTableHashCode(&newTag);
    newPartitionLock = BufMappingPartitionLock(newHash);

    // Check if block is already in buffer pool
    LWLockAcquire(newPartitionLock, LW_SHARED);
    buf_id = BufTableLookup(&newTag, newHash);
    LWLockRelease(newPartitionLock);

    if (buf_id < 0) {
        // Block not in cache - initiate async I/O if prefetch enabled
        if ((io_direct_flags & IO_DIRECT_DATA) == 0 &&
            smgrprefetch(smgr_reln, forkNum, blockNum, 1)) {
            result.initiated_io = true;
        }
    } else {
        // Block found in cache - return buffer ID for potential optimization
        result.recent_buffer = buf_id + 1;
    }

    return result;
}
```