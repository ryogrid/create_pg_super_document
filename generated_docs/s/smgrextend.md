# smgrextend

## Location
[src/backend/storage/smgr/smgr.c:535-559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L535-L559)

## Overview
The smgrextend function adds a new block to a PostgreSQL relation file, extending the file beyond its current end-of-file (EOF) position.

## Definition

```c
void
smgrextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   const void *buffer, bool skipFsync)
```
## Detailed Description
The smgrextend function is a storage manager interface for extending relation files by adding new blocks. It is specifically designed for cases where the block number is at or beyond the current EOF of the file. The function delegates the actual extension operation to the appropriate storage manager implementation through the smgrsw function pointer table. After extending the file, it updates the cached block count to maintain consistency, assuming that writing beyond EOF fills intervening space with zeroes.

## Parameters / Member Variables
- `reln`: SMgrRelation pointer identifying the relation to extend
- `forknum`: ForkNumber indicating which fork of the relation to extend (main, FSM, VM, etc.)
- `blocknum`: BlockNumber specifying the position where the new block should be written
- `*buffer`: Pointer to the data buffer containing the block content to write
- `skipFsync`: Boolean flag indicating whether to skip filesystem synchronization
## Dependencies
- Functions called/Symbols referenced:
  - smgrsw[].smgr_extend (storage manager implementation function)
  - SMgrRelation (relation structure)
  - InvalidBlockNumber (constant for invalid block number)
- Called from (representative examples):
  - [_hash_alloc_buckets](../h/_hash_alloc_buckets.md) (hash index bucket allocation)
  - [RelationCopyStorageUsingBuffer](../R/RelationCopyStorageUsingBuffer.md) (relation copying utility)
  - [smgr_bulk_flush](smgr_bulk_flush.md) (bulk write operations)

## Notes and Other Information
- The function assumes that writing beyond current EOF automatically fills intervening file space with zeroes
- Updates the cached block count (smgr_cached_nblocks) optimistically, invalidating it if the expectation doesn't match
- Part of the storage manager abstraction layer, allowing different storage implementations
- Critical for relation extension operations during table growth and index expansion

## Simplified Source

```c
void smgrextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                const void *buffer, bool skipFsync)
{
    // Delegate to storage manager implementation
    smgrsw[reln->smgr_which].smgr_extend(reln, forknum, blocknum, buffer, skipFsync);

    // Update cached block count
    if (reln->smgr_cached_nblocks[forknum] == blocknum)
        reln->smgr_cached_nblocks[forknum] = blocknum + 1;  // Expected extension
    else
        reln->smgr_cached_nblocks[forknum] = InvalidBlockNumber;  // Invalidate cache
}
```