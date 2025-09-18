# BlockRefTableEntrySetLimitBlock

## Location
[src/common/blkreftable.c:894-964](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L894-L964)

## Overview
BlockRefTableEntrySetLimitBlock updates a BlockRefTableEntry with a new limit block value and removes any tracked block modifications at or above that limit to maintain consistency with the relation's known length.

## Definition
```c
void BlockRefTableEntrySetLimitBlock(BlockRefTableEntry *entry, BlockNumber limit_block)
```

## Detailed Description
This function sets the "limit block" for a BlockRefTableEntry, which represents the shortest known length of the relation within the range of WAL records covered by the block reference table. When a limit block is set, all tracked block modifications at or above that block number become invalid and must be removed, as those blocks are considered to exist beyond the relation's known length.

The function performs several optimization checks: it ignores requests to set a higher limit block, discards entire chunks that are completely above the limit, and handles partial chunk cleanup differently based on whether the chunk stores data as a bitmap (when full) or as an offset array (when sparse). The algorithm ensures that only blocks below the limit remain tracked as modified.

## Parameters / Member Variables
- `entry`: The BlockRefTableEntry to update with the new limit block
- `limit_block`: The new limit block number representing the relation's maximum known length

## Dependencies
- Functions called/Symbols referenced:
  - BlockRefTableEntry (entry structure type)
  - BlockRefTableChunk (chunk data type)
  - BLOCKS_PER_CHUNK (blocks stored per chunk constant)
  - MAX_ENTRIES_PER_CHUNK (maximum entries indicating bitmap mode)
  - BLOCKS_PER_ENTRY (blocks represented per bitmap entry)
  - Assert (assertion macro for debugging)

- Called from (representative examples):
  - BlockRefTableSetLimitBlock (higher-level limit block setting)
  - WAL processing code when relations are truncated
  - Backup utilities that need to handle relation size changes

## Notes and Other Information
- Only processes limit blocks that are lower than the current limit (optimizes redundant calls)
- Handles two different chunk storage formats: sparse offset arrays and dense bitmaps
- Chunks beyond the limit block are completely cleared by setting usage to 0
- Bitmap chunks have individual bits cleared for blocks >= limit_block
- Offset array chunks are compacted by filtering out large offsets
- The limit block represents the shortest known relation length during the WAL range
- Critical for maintaining consistency when relations are truncated or dropped
- Part of the block reference table's mechanism for tracking relation size changes during WAL replay