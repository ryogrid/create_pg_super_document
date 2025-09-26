# BlockRefTableMarkBlockModified

## Location
src/common/blkreftable.c: 297 - 339

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
- : Pointer to the BlockRefTable to modify
- : Pointer to RelFileLocator identifying the specific relation
- : Fork number (main, fsm, vm, etc.) within the relation
- : Block number that has been modified

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