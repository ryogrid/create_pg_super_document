# BlockRefTableSetLimitBlock

## Location
src/common/blkreftable.c: 262 - 296

## Overview
Sets the "limit block" for a relation fork and removes any tracked modified blocks with equal or higher block numbers.

## Definition
```c
void BlockRefTableSetLimitBlock(BlockRefTable *brtab,
                               const RelFileLocator *rlocator,
                               ForkNumber forknum,
                               BlockNumber limit_block)
```

## Detailed Description
This function establishes a "limit block" for a specific relation fork within a block reference table. The limit block represents the shortest known length of the relation within the range of WAL records covered by the block reference table. When setting this limit, the function automatically discards any information about modified blocks that have block numbers equal to or higher than the specified limit.

The function operates by:
1. Creating a key from the relation locator and fork number
2. Looking up or creating an entry in the hash table
3. If no entry exists, initializing a new entry with the limit block value
4. If an entry exists, calling BlockRefTableEntrySetLimitBlock to update the limit and clean up higher-numbered blocks

## Parameters / Member Variables
- : Pointer to the BlockRefTable to modify
- : Pointer to RelFileLocator identifying the specific relation
- : Fork number (main, fsm, vm, etc.) within the relation
- : Block number representing the new limit for the relation fork

## Dependencies
- Functions called/Symbols referenced:
  - : Copies the RelFileLocator to the key structure
  - : Inserts or finds an entry in the hash table
  - : Updates existing entry's limit block
- Called from (representative examples):
  - : During incremental backup preparation
  - : When processing database-related WAL records
  - : When processing storage manager WAL records
  - : When processing transaction WAL records

## Notes and Other Information
- The limit block concept is crucial for incremental backups, ensuring that only valid blocks within the known relation length are tracked
- The function handles both new entries (initialization) and existing entries (limit adjustment)
- The BlockRefTableKey structure is zero-initialized to ensure consistent padding
- This operation can potentially free memory by removing tracking information for blocks beyond the limit
- The function is commonly used during WAL summarization to maintain accurate relation size information