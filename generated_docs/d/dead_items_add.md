# dead_items_add

## Location
[src/backend/access/heap/vacuumlazy.c:2888-2909](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L2888-L2909)

## Overview
 is a static utility function in PostgreSQL's VACUUM implementation that adds dead tuple identifiers (TIDs) to the dead items collection and updates vacuum progress statistics.

## Definition

```c
static void
dead_items_add(LVRelState *vacrel, BlockNumber blkno, OffsetNumber *offsets,
			   int num_offsets)
```
## Detailed Description
This function serves as a centralized mechanism for recording dead tuple locations during VACUUM operations. It takes a block number and an array of offset numbers within that block, then stores these tuple identifiers in the vacuum relation state's dead items collection using TidStore. Additionally, it maintains accurate progress reporting by updating vacuum statistics including the total number of dead items and memory usage.

The function is designed to be called during heap scanning phases of VACUUM when dead tuples are identified and need to be tracked for later cleanup operations.

## Parameters / Member Variables
- `*vacrel`: Pointer to LVRelState structure containing vacuum operation state and configuration
- `blkno`: Block number within the relation where dead tuples were found
- `*offsets`: Array of offset numbers identifying specific tuple positions within the block
- `num_offsets`: Number of offset entries in the offsets array
## Dependencies
- Functions called/Symbols referenced:
  - [TidStoreSetBlockOffsets](../T/TidStoreSetBlockOffsets.md)
  - [TidStoreMemoryUsage](../T/TidStoreMemoryUsage.md)
  - [pgstat_progress_update_multi_param](../p/pgstat_progress_update_multi_param.md)
  - PROGRESS_VACUUM_NUM_DEAD_ITEM_IDS
  - PROGRESS_VACUUM_DEAD_TUPLE_BYTES
- Called from (representative examples):
  - [lazy_scan_prune](../l/lazy_scan_prune.md)
  - [lazy_scan_noprune](../l/lazy_scan_noprune.md)

## Notes and Other Information
- This is a static function, only accessible within vacuumlazy.c
- The function updates two progress statistics atomically using pgstat_progress_update_multi_param
- Memory usage tracking helps monitor vacuum's resource consumption during operation
- The dead items collection is used later in the vacuum process for index cleanup and heap cleanup phases