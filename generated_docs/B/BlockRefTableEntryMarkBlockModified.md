# BlockRefTableEntryMarkBlockModified

## Location
[src/common/blkreftable.c:965-1121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L965-L1121)

## Overview
Marks a specific block in a BlockRefTableEntry as known to have been modified, managing the internal data structures that track block modifications.

## Definition

```c
void
BlockRefTableEntryMarkBlockModified(BlockRefTableEntry *entry,
									ForkNumber forknum,
									BlockNumber blknum)
```
## Detailed Description
This function updates a BlockRefTableEntry to record that a specific block has been modified. It implements an adaptive storage strategy that efficiently handles both sparse and dense modification patterns. The function manages chunks of block references, where each chunk can store block numbers either as an array (for sparse modifications) or as a bitmap (for dense modifications). When the number of modified blocks in a chunk reaches a threshold, it automatically converts from array format to bitmap format for better memory efficiency.

The function handles dynamic allocation and reallocation of chunk arrays when new blocks need to be tracked. It ensures that the data structures can grow to accommodate any valid block number within the relation.

## Parameters / Member Variables
- : Pointer to the BlockRefTableEntry that will be updated to track the modified block
- : Fork number identifying which fork of the relation contains the modified block
- : Block number within the fork that has been modified

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [repalloc](../r/repalloc.md)
  - [pfree](../p/pfree.md)
  - Max
  - Assert
- Called from (representative examples):
  - BlockRefTableMarkBlockModified

## Notes and Other Information
- Uses adaptive storage: arrays for sparse block modifications, bitmaps for dense modifications
- Automatically converts from array to bitmap format when MAX_ENTRIES_PER_CHUNK - 1 entries are reached
- Dynamically grows chunk arrays as needed, doubling the size each time
- Initial chunk allocation starts with INITIAL_ENTRIES_PER_CHUNK entries
- Chunk numbers are calculated as blknum / BLOCKS_PER_CHUNK
- Block offsets within chunks are calculated as blknum % BLOCKS_PER_CHUNK
- Memory allocation strategy minimizes waste for both sparse and dense modification patterns