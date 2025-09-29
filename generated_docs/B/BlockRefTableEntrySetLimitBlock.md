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
  - [BlockRefTableEntry](BlockRefTableEntry.md) (entry structure type)
  - BlockRefTableChunk (chunk data type)
  - BLOCKS_PER_CHUNK (blocks stored per chunk constant)
  - MAX_ENTRIES_PER_CHUNK (maximum entries indicating bitmap mode)
  - BLOCKS_PER_ENTRY (blocks represented per bitmap entry)
  - Assert (assertion macro for debugging)

- Called from (representative examples):
  - [BlockRefTableSetLimitBlock](BlockRefTableSetLimitBlock.md) (higher-level limit block setting)
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

## Simplified Source

```c
void
BlockRefTableEntrySetLimitBlock(BlockRefTableEntry *entry,
                               BlockNumber limit_block)
{
    unsigned chunkno, limit_chunkno, limit_chunkoffset;

    // Only process if new limit is lower than current limit
    if (limit_block >= entry->limit_block)
        return;

    // Update the limit block value
    entry->limit_block = limit_block;

    // Calculate which chunk contains the limit block
    limit_chunkno = limit_block / BLOCKS_PER_CHUNK;
    limit_chunkoffset = limit_block % BLOCKS_PER_CHUNK;

    // Nothing to do if limit chunk is beyond our current chunks
    if (limit_chunkno >= entry->nchunks)
        return;

    // Clear all chunks beyond the limit chunk
    for (chunkno = limit_chunkno + 1; chunkno < entry->nchunks; ++chunkno)
        entry->chunk_usage[chunkno] = 0;

    // Handle the limit chunk based on its storage format
    BlockRefTableChunk limit_chunk = entry->chunk_data[limit_chunkno];

    if (entry->chunk_usage[limit_chunkno] == MAX_ENTRIES_PER_CHUNK) {
        // Bitmap format: clear bits for blocks >= limit
        for (unsigned chunkoffset = limit_chunkoffset;
             chunkoffset < BLOCKS_PER_CHUNK;
             ++chunkoffset) {
            limit_chunk[chunkoffset / BLOCKS_PER_ENTRY] &=
                ~(1 << (chunkoffset % BLOCKS_PER_ENTRY));
        }
    } else {
        // Array format: filter out offsets >= limit
        unsigned j = 0;
        for (unsigned i = 0; i < entry->chunk_usage[limit_chunkno]; ++i) {
            if (limit_chunk[i] < limit_chunkoffset)
                limit_chunk[j++] = limit_chunk[i];
        }
        entry->chunk_usage[limit_chunkno] = j;
    }
}
```