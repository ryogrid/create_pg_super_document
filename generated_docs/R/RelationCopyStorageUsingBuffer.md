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
  - smgrnblocks, smgropen, smgrextend
  - GetAccessStrategy, FreeAccessStrategy
  - ReadBufferWithoutRelcache
  - LockBuffer, UnlockReleaseBuffer
  - BufferGetPage, MarkBufferDirty
  - log_newpage_buffer
  - START_CRIT_SECTION, END_CRIT_SECTION
- Called from (representative examples):
  - CreateAndCopyRelationData

## Notes and Other Information
- This is a static function in bufmgr.c, indicating it's an internal implementation detail
- Uses bulk access strategies (BAS_BULKREAD/BAS_BULKWRITE) to optimize buffer pool usage during large copy operations
- WAL logging is conditional: skipped for unlogged relations except init fork, always done for permanent relations when WAL is enabled
- The function pre-extends the destination to the full size before copying to avoid repeated extension operations
- Critical sections ensure atomicity of page copy and WAL logging operations