# fsm_does_block_exist

## Location
src/backend/storage/freespace/freespace.c: 926 - 940

## Overview
Static utility function that checks whether a given block number exists within a relation, handling edge cases that can occur during WAL replay operations.

## Definition
```c
static bool fsm_does_block_exist(Relation rel, BlockNumber blknumber)
```

## Detailed Description
This function determines whether a specified block number is valid and exists within a relation. It addresses a specific edge case that can occur during Write-Ahead Logging (WAL) replay: the Free Space Map may have been written to disk and reference newly-extended pages, but those pages themselves may not have been flushed to disk yet.

The function implements a two-tier checking strategy:
1. **Fast path**: If the block number is below the cached nblocks value in the storage manager, the block definitely exists
2. **Fallback path**: If not covered by the cache or cache is invalid, performs a fresh check using RelationGetNumberOfBlocks(), which incurs lseek() system call overhead

This approach trades some performance (lseek overhead) for correctness, ensuring that the FSM doesn't incorrectly assume blocks don't exist when they were just extended by the main fork.

## Parameters / Member Variables
- `rel`: The relation to check for block existence
- `blknumber`: The block number to verify

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetSmgr
  - BlockNumberIsValid
  - RelationGetNumberOfBlocks
  - MAIN_FORKNUM (constant)
- Called from (representative examples):
  - RecordAndGetPageWithFreeSpace
  - fsm_search

## Notes and Other Information
- The function is static and only used internally within the freespace.c module
- Returns true if the block exists, false otherwise  
- Designed specifically to handle WAL replay scenarios where FSM and data pages may be out of sync
- The cached nblocks optimization avoids expensive system calls for most common cases
- Critical for maintaining FSM accuracy during crash recovery operations
- The trade-off between performance and correctness favors correctness to prevent FSM inconsistencies