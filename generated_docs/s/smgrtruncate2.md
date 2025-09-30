# smgrtruncate2

## Location
[src/backend/storage/smgr/smgr.c:727-782](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L727-L782)

## Overview
Truncates the specified forks of a supplied relation to given numbers of blocks, performing immediate truncation with proper buffer management and cache invalidation.

## Definition

```c
void
smgrtruncate2(SMgrRelation reln, ForkNumber *forknum, int nforks,
			  BlockNumber *old_nblocks, BlockNumber *nblocks)
```
## Detailed Description
The  function performs immediate truncation of multiple forks of a storage manager relation. It implements a comprehensive truncation process that includes dropping relation buffers for the to-be-deleted blocks, sending shared invalidation messages to other backends, and updating cached block counts. The function is designed to be called within a critical section and requires the caller to hold AccessExclusiveLock on the relation. The truncation cannot be rolled back as it immediately modifies the physical storage.

## Parameters / Member Variables
- : SMgrRelation pointer representing the storage manager relation to truncate  
- : Array of ForkNumber values indicating which forks to truncate
- : Integer specifying the number of forks in the arrays
- : Array of BlockNumber values containing the current size of each fork
- : Array of BlockNumber values specifying the target size for each corresponding fork

## Dependencies
- Functions called/Symbols referenced:
  - SMgrRelation (type)
  - [DropRelationBuffers](../D/DropRelationBuffers.md)
  - [CacheInvalidateSmgr](../C/CacheInvalidateSmgr.md)
  - InvalidBlockNumber
  - smgrsw (storage manager dispatch table)
- Called from (representative examples):
  - [RelationTruncate](../R/RelationTruncate.md)
  - [smgr_redo](smgr_redo.md)
  - [smgrtruncate](smgrtruncate.md)
  - SmgrIsTemp

## Notes and Other Information
- Performs immediate truncation that cannot be rolled back
- Requires caller to hold AccessExclusiveLock on the relation
- Should normally be called in a critical section
- Current size must be checked outside the critical section before calling
- Drops relation buffers for blocks that will be deleted without writing contents
- Sends shared invalidation messages to force other backends to close smgr references
- Updates local cached block counts after successful truncation
- The cached size is invalidated first to handle potential errors during truncation
- Located in src/backend/storage/smgr/smgr.c:727-782

## Simplified Source

```c
void
smgrtruncate2(SMgrRelation reln, ForkNumber *forknum, int nforks,
              BlockNumber *old_nblocks, BlockNumber *nblocks)
{
    int i;

    // Drop buffers for blocks about to be deleted
    DropRelationBuffers(reln, forknum, nforks, nblocks);

    // Send invalidation message to other backends
    CacheInvalidateSmgr(reln->smgr_rlocator);

    // Truncate each fork
    for (i = 0; i < nforks; i++) {
        // Invalidate cached size in case of error
        reln->smgr_cached_nblocks[forknum[i]] = InvalidBlockNumber;

        // Perform the actual truncation
        smgrsw[reln->smgr_which].smgr_truncate(reln, forknum[i],
                                               old_nblocks[i], nblocks[i]);

        // Update cached size after successful truncation
        reln->smgr_cached_nblocks[forknum[i]] = nblocks[i];
    }
}
```