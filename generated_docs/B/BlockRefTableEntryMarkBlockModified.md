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
- `*entry`: Pointer to the BlockRefTableEntry that will be updated to track the modified block
- `forknum`: Fork number identifying which fork of the relation contains the modified block
- `blknum`: Block number within the fork that has been modified
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [repalloc](../r/repalloc.md)
  - [pfree](../p/pfree.md)
  - Max
  - Assert
- Called from (representative examples):
  - [BlockRefTableMarkBlockModified](BlockRefTableMarkBlockModified.md)

## Notes and Other Information
- Uses adaptive storage: arrays for sparse block modifications, bitmaps for dense modifications
- Automatically converts from array to bitmap format when MAX_ENTRIES_PER_CHUNK - 1 entries are reached
- Dynamically grows chunk arrays as needed, doubling the size each time
- Initial chunk allocation starts with INITIAL_ENTRIES_PER_CHUNK entries
- Chunk numbers are calculated as blknum / BLOCKS_PER_CHUNK
- Block offsets within chunks are calculated as blknum % BLOCKS_PER_CHUNK
- Memory allocation strategy minimizes waste for both sparse and dense modification patterns

## Simplified Source

```c
void
BlockRefTableEntryMarkBlockModified(BlockRefTableEntry *entry,
                                   ForkNumber forknum,
                                   BlockNumber blknum)
{
    unsigned chunkno, chunkoffset, i;

    // Calculate which chunk this block belongs to
    chunkno = blknum / BLOCKS_PER_CHUNK;
    chunkoffset = blknum % BLOCKS_PER_CHUNK;

    // Expand chunk arrays if needed
    if (chunkno >= entry->nchunks) {
        unsigned max_chunks = Max(16, entry->nchunks);
        while (max_chunks < chunkno + 1)
            max_chunks *= 2;

        // Allocate or reallocate chunk arrays
        if (entry->nchunks == 0) {
            entry->chunk_size = palloc0(sizeof(uint16) * max_chunks);
            entry->chunk_usage = palloc0(sizeof(uint16) * max_chunks);
            entry->chunk_data = palloc0(sizeof(BlockRefTableChunk) * max_chunks);
        } else {
            // Reallocate and clear new entries
            entry->chunk_size = repalloc(entry->chunk_size, sizeof(uint16) * max_chunks);
            entry->chunk_usage = repalloc(entry->chunk_usage, sizeof(uint16) * max_chunks);
            entry->chunk_data = repalloc(entry->chunk_data, sizeof(BlockRefTableChunk) * max_chunks);
        }
        entry->nchunks = max_chunks;
    }

    // Create new chunk if it doesn't exist
    if (entry->chunk_size[chunkno] == 0) {
        entry->chunk_data[chunkno] = palloc(sizeof(uint16) * INITIAL_ENTRIES_PER_CHUNK);
        entry->chunk_size[chunkno] = INITIAL_ENTRIES_PER_CHUNK;
        entry->chunk_data[chunkno][0] = chunkoffset;
        entry->chunk_usage[chunkno] = 1;
        return;
    }

    // If chunk is a bitmap (max entries), set the appropriate bit
    if (entry->chunk_usage[chunkno] == MAX_ENTRIES_PER_CHUNK) {
        BlockRefTableChunk chunk = entry->chunk_data[chunkno];
        chunk[chunkoffset / BLOCKS_PER_ENTRY] |= 1 << (chunkoffset % BLOCKS_PER_ENTRY);
        return;
    }

    // Check if block is already marked in array format
    for (i = 0; i < entry->chunk_usage[chunkno]; ++i) {
        if (entry->chunk_data[chunkno][i] == chunkoffset)
            return; // Already marked
    }

    // Convert to bitmap if array is nearly full
    if (entry->chunk_usage[chunkno] == MAX_ENTRIES_PER_CHUNK - 1) {
        BlockRefTableChunk newchunk = palloc0(MAX_ENTRIES_PER_CHUNK * sizeof(uint16));

        // Convert existing array entries to bitmap
        for (unsigned j = 0; j < entry->chunk_usage[chunkno]; ++j) {
            unsigned coff = entry->chunk_data[chunkno][j];
            newchunk[coff / BLOCKS_PER_ENTRY] |= 1 << (coff % BLOCKS_PER_ENTRY);
        }

        // Add new entry to bitmap
        newchunk[chunkoffset / BLOCKS_PER_ENTRY] |= 1 << (chunkoffset % BLOCKS_PER_ENTRY);

        // Replace old chunk with bitmap
        pfree(entry->chunk_data[chunkno]);
        entry->chunk_data[chunkno] = newchunk;
        entry->chunk_size[chunkno] = MAX_ENTRIES_PER_CHUNK;
        entry->chunk_usage[chunkno] = MAX_ENTRIES_PER_CHUNK;
        return;
    }

    // Expand array if needed
    if (entry->chunk_usage[chunkno] == entry->chunk_size[chunkno]) {
        unsigned newsize = entry->chunk_size[chunkno] * 2;
        entry->chunk_data[chunkno] = repalloc(entry->chunk_data[chunkno], newsize * sizeof(uint16));
        entry->chunk_size[chunkno] = newsize;
    }

    // Add new entry to array
    entry->chunk_data[chunkno][entry->chunk_usage[chunkno]] = chunkoffset;
    entry->chunk_usage[chunkno]++;
}
```