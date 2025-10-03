# smgrzeroextend

## Location
[src/backend/storage/smgr/smgr.c:560-584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L560-L584)

## Overview
The smgrzeroextend function extends a PostgreSQL relation file by adding multiple new blocks filled with zeroes in a single operation.

## Definition

```c
void
smgrzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			   int nblocks, bool skipFsync)
```
## Detailed Description
The smgrzeroextend function is a storage manager interface for efficiently extending relation files by multiple blocks at once, with all added blocks being zero-filled. This function is similar to smgrextend but optimized for bulk extension operations. It delegates the actual zero-extension operation to the appropriate storage manager implementation through the smgrsw function pointer table. After extending the file, it updates the cached block count by the number of blocks added, maintaining consistency with the file system state.

## Parameters / Member Variables
- `reln`: SMgrRelation pointer identifying the relation to extend
- `forknum`: ForkNumber indicating which fork of the relation to extend (main, FSM, VM, etc.)
- `blocknum`: BlockNumber specifying the starting position for the new blocks
- `nblocks`: Integer count of blocks to add to the file
- `skipFsync`: Boolean flag indicating whether to skip filesystem synchronization
## Dependencies
- Functions called/Symbols referenced:
  - smgrsw[].smgr_zeroextend (storage manager implementation function)
  - SMgrRelation (relation structure)
  - InvalidBlockNumber (constant for invalid block number)
- Called from (representative examples):
  - [ExtendBufferedRelShared](../E/ExtendBufferedRelShared.md) (shared buffer extension)
  - [ExtendBufferedRelLocal](../E/ExtendBufferedRelLocal.md) (local buffer extension)

## Notes and Other Information
- More efficient than calling smgrextend multiple times for bulk extensions
- All added blocks are guaranteed to be filled with zeroes
- Updates the cached block count optimistically by adding nblocks to the current position
- Invalidates the cache if the expected block count doesn't match actual state
- Part of the storage manager abstraction layer supporting different storage implementations
- Commonly used in buffer management for extending relations efficiently

## Simplified Source

```c
void
smgrzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
               int nblocks, bool skipFsync)
{
    // Delegate to the appropriate storage manager implementation
    smgrsw[reln->smgr_which].smgr_zeroextend(reln, forknum, blocknum,
                                            nblocks, skipFsync);

    // Update cached block count if it matches expected value
    if (reln->smgr_cached_nblocks[forknum] == blocknum)
        reln->smgr_cached_nblocks[forknum] = blocknum + nblocks;
    else
        reln->smgr_cached_nblocks[forknum] = InvalidBlockNumber;
}
```