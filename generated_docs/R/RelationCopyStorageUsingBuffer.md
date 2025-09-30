# RelationCopyStorageUsingBuffer

## Location
[src/backend/storage/buffer/bufmgr.c:4680-4770](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L4680-L4770)

## Overview
RelationCopyStorageUsingBuffer copies fork data between two relations using buffer manager APIs instead of direct storage manager calls, providing better integration with PostgreSQL's buffer management system.

## Definition
```c
static void RelationCopyStorageUsingBuffer(RelFileLocator srclocator,
                                         RelFileLocator dstlocator,
                                         ForkNumber forkNum, 
                                         bool permanent)
```

## Detailed Description
This function provides an alternative implementation to RelationCopyStorage by using buffer manager APIs (ReadBufferWithoutRelcache) instead of direct storage manager calls (smgrread/smgrextend). The function performs a block-by-block copy operation with the following key features:

- Uses bulk read/write buffer access strategies for optimal performance
- Implements WAL logging when appropriate based on wal_level and relation type
- Handles both permanent and temporary relations correctly
- Pre-extends the destination relation to avoid incremental extension overhead
- Operates within critical sections to ensure data consistency

The function respects PostgreSQL's WAL-before-data rule and logs new pages when necessary for crash recovery.

## Parameters / Member Variables
- `srclocator`: RelFileLocator identifying the source relation file
- `dstlocator`: RelFileLocator identifying the destination relation file  
- `forkNum`: Fork number specifying which fork to copy (main, FSM, VM, init)
- `permanent`: Boolean indicating if the relation is permanent (affects WAL logging decisions)

## Dependencies
- Functions called/Symbols referenced:
  - XLogIsNeeded
  - [smgrnblocks](../s/smgrnblocks.md), smgropen, smgrextend
  - [GetAccessStrategy](../G/GetAccessStrategy.md), FreeAccessStrategy
  - [ReadBufferWithoutRelcache](ReadBufferWithoutRelcache.md)
  - [LockBuffer](../L/LockBuffer.md), UnlockReleaseBuffer
  - [BufferGetPage](../B/BufferGetPage.md), MarkBufferDirty
  - [log_newpage_buffer](../l/log_newpage_buffer.md)
  - START_CRIT_SECTION, END_CRIT_SECTION
- Called from (representative examples):
  - [CreateAndCopyRelationData](../C/CreateAndCopyRelationData.md)

## Notes and Other Information
- This is a static function in bufmgr.c, indicating it's an internal implementation detail
- Uses bulk access strategies (BAS_BULKREAD/BAS_BULKWRITE) to optimize buffer pool usage during large copy operations
- WAL logging is conditional: skipped for unlogged relations except init fork, always done for permanent relations when WAL is enabled
- The function pre-extends the destination to the full size before copying to avoid repeated extension operations
- Critical sections ensure atomicity of page copy and WAL logging operations

## Simplified Source

```c
static void
RelationCopyStorageUsingBuffer(RelFileLocator srclocator,
                              RelFileLocator dstlocator,
                              ForkNumber forkNum, bool permanent)
{
    Buffer srcBuf, dstBuf;
    Page srcPage, dstPage;
    bool use_wal;
    BlockNumber nblocks, blkno;
    BufferAccessStrategy bstrategy_src, bstrategy_dst;

    // Determine if WAL logging is needed
    use_wal = XLogIsNeeded() && (permanent || forkNum == INIT_FORKNUM);

    // Get source relation size
    nblocks = smgrnblocks(smgropen(srclocator, INVALID_PROC_NUMBER), forkNum);
    if (nblocks == 0)
        return;

    // Pre-extend destination to full size
    memset(buf.data, 0, BLCKSZ);
    smgrextend(smgropen(dstlocator, INVALID_PROC_NUMBER), forkNum,
               nblocks - 1, buf.data, true);

    // Setup bulk access strategies for performance
    bstrategy_src = GetAccessStrategy(BAS_BULKREAD);
    bstrategy_dst = GetAccessStrategy(BAS_BULKWRITE);

    // Copy each block from source to destination
    for (blkno = 0; blkno < nblocks; blkno++)
    {
        // Read source block
        srcBuf = ReadBufferWithoutRelcache(srclocator, forkNum, blkno,
                                          RBM_NORMAL, bstrategy_src, permanent);
        LockBuffer(srcBuf, BUFFER_LOCK_SHARE);
        srcPage = BufferGetPage(srcBuf);

        // Read destination block (zero-initialized)
        dstBuf = ReadBufferWithoutRelcache(dstlocator, forkNum, blkno,
                                          RBM_ZERO_AND_LOCK, bstrategy_dst, permanent);
        dstPage = BufferGetPage(dstBuf);

        START_CRIT_SECTION();

        // Copy page data
        memcpy(dstPage, srcPage, BLCKSZ);
        MarkBufferDirty(dstBuf);

        // Log if WAL is needed
        if (use_wal)
            log_newpage_buffer(dstBuf, true);

        END_CRIT_SECTION();

        UnlockReleaseBuffer(dstBuf);
        UnlockReleaseBuffer(srcBuf);
    }

    FreeAccessStrategy(bstrategy_src);
    FreeAccessStrategy(bstrategy_dst);
}
```