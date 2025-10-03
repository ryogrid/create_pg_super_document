# BlockRefTableMarkBlockModified

## Location
[src/common/blkreftable.c:297-339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L297-L339)

## Overview
Marks a specific block in a relation fork as having been modified, tracking it within the block reference table for incremental backup purposes.

## Definition
```c
void BlockRefTableMarkBlockModified(BlockRefTable *brtab,
                                   const RelFileLocator *rlocator,
                                   ForkNumber forknum,
                                   BlockNumber blknum)
```

## Detailed Description
This function records that a specific block within a relation fork has been modified. It maintains a data structure that tracks which blocks have been changed, which is essential for incremental backup operations. The function handles both new relation fork entries and updates to existing entries.

The function operates by:
1. Creating a lookup key from the relation locator and fork number
2. Inserting or finding the corresponding entry in the hash table
3. For new entries, initializing with InvalidBlockNumber as the limit block
4. Delegating the actual block marking to BlockRefTableEntryMarkBlockModified

Memory context management is handled differently between frontend and backend environments, with backend versions explicitly switching to the table's memory context before allocations.

## Parameters / Member Variables
- `*brtab`: Pointer to the BlockRefTable to modify
- `*rlocator`: Pointer to RelFileLocator identifying the specific relation
- `forknum`: Fork number (main, fsm, vm, etc.) within the relation
- `blknum`: Block number that has been modified
## Dependencies
- Functions called/Symbols referenced:
  - : Switches memory context for allocations (backend only)
  - : Copies the RelFileLocator to the key structure
  - : Inserts or finds an entry in the hash table
  - : Marks the specific block as modified
  - : Constant used for initial limit block value
- Called from (representative examples):
  - : During incremental backup preparation
  - : When processing WAL records to identify modified blocks

## Notes and Other Information
- New entries are initialized with InvalidBlockNumber as the limit block, which is higher than any legal block number
- Memory context switching ensures proper memory allocation in backend environments
- The function provides the high-level interface for block modification tracking, with the actual implementation delegated to entry-specific functions
- This is a core function for incremental backup functionality, as it builds the map of modified blocks
- The zero-initialized key structure ensures consistent padding for hash table operations
- Frontend compilation excludes memory context management code since it's not available in frontend utilities

## Simplified Source

```c
void
BlockRefTableMarkBlockModified(BlockRefTable *brtab,
                              const RelFileLocator *rlocator,
                              ForkNumber forknum,
                              BlockNumber blknum)
{
    BlockRefTableEntry *brtentry;
    BlockRefTableKey key = {{0}};  // Zero-initialize for consistent hashing
    bool found;

#ifndef FRONTEND
    // Switch to table's memory context for allocations
    MemoryContext oldcontext = MemoryContextSwitchTo(brtab->mcxt);
#endif

    // Build lookup key for hash table
    memcpy(&key.rlocator, rlocator, sizeof(RelFileLocator));
    key.forknum = forknum;

    // Find or create entry for this relation fork
    brtentry = blockreftable_insert(brtab->hash, key, &found);

    if (!found)
    {
        // Initialize new entry
        brtentry->limit_block = InvalidBlockNumber;  // Higher than any legal block
        brtentry->nchunks = 0;
        brtentry->chunk_size = NULL;
        brtentry->chunk_usage = NULL;
        brtentry->chunk_data = NULL;
    }

    // Mark the specific block as modified
    BlockRefTableEntryMarkBlockModified(brtentry, forknum, blknum);

#ifndef FRONTEND
    // Restore previous memory context
    MemoryContextSwitchTo(oldcontext);
#endif
}
```