# BlockRefTableEntryGetBlocks

## Location
[src/common/blkreftable.c:369-473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L369-L473)

## Overview
Extracts modified block numbers from a block reference table entry within a specified range, handling both bitmap and offset array storage formats.

## Definition
```c
int BlockRefTableEntryGetBlocks(BlockRefTableEntry *entry,
                               BlockNumber start_blkno,
                               BlockNumber stop_blkno,
                               BlockNumber *blocks,
                               int nblocks)
```

## Detailed Description
This function retrieves block numbers that have been marked as modified within a BlockRefTableEntry, filtering them to return only those that fall within the specified range [start_blkno, stop_blkno). The function handles the internal storage format transparently, whether the data is stored as a bitmap (for dense populations) or as an array of offsets (for sparse populations).

The function operates by:
1. Calculating which chunks might contain blocks within the specified range
2. Iterating through relevant chunks and examining their storage format
3. For bitmap format: testing each bit within the range
4. For offset array format: checking each stored offset against the range
5. Converting chunk-relative positions back to absolute block numbers
6. Early termination when the output buffer is full

The implementation includes careful overflow handling since stop_blkno could be InvalidBlockNumber (maximum value).

## Parameters / Member Variables
- `*entry`: Pointer to the BlockRefTableEntry to read from (must not be NULL)
- `start_blkno`: First block number to include in results (inclusive)
- `stop_blkno`: Block number to stop at (exclusive)
- `*blocks`: Output array to store found block numbers (must have space for nblocks)
- `nblocks`: Maximum number of block numbers that can be stored in blocks array
## Dependencies
- Functions called/Symbols referenced:
  - : Assertion macros for validation
  - : Constant defining chunk size
  - : Constant indicating bitmap storage mode
  - : Constant for bitmap entry size
  - : Type for chunk data storage
- Called from (representative examples):
  - : During incremental backup to determine which blocks need backing up

## Notes and Other Information
- Returns the actual number of block numbers written to the blocks array
- Handles two internal storage formats transparently: bitmap for dense block populations, offset arrays for sparse ones
- The function performs range filtering, only returning blocks where start_blkno ≤ block < stop_blkno
- Includes overflow protection when calculating chunk boundaries, important when stop_blkno is InvalidBlockNumber
- Early termination occurs when the output buffer is full, preventing buffer overruns
- Chunk-based storage allows efficient representation of both sparse and dense block modification patterns
- The bitmap format uses individual bits to represent block modification status
- The offset array format stores actual block offsets within chunks for sparse modification patterns
- This function is critical for incremental backup operations, determining exactly which blocks have been modified

## Simplified Source

```c
int BlockRefTableEntryGetBlocks(BlockRefTableEntry *entry,
                               BlockNumber start_blkno,
                               BlockNumber stop_blkno,
                               BlockNumber *blocks,
                               int nblocks)
{
    uint32 start_chunkno;
    uint32 stop_chunkno;
    uint32 chunkno;
    int nresults = 0;

    Assert(entry != NULL);

    // Calculate which chunks could contain blocks of interest
    start_chunkno = start_blkno / BLOCKS_PER_CHUNK;
    stop_chunkno = stop_blkno / BLOCKS_PER_CHUNK;
    if ((stop_blkno % BLOCKS_PER_CHUNK) != 0)
        ++stop_chunkno;
    if (stop_chunkno > entry->nchunks)
        stop_chunkno = entry->nchunks;

    // Process each relevant chunk
    for (chunkno = start_chunkno; chunkno < stop_chunkno; ++chunkno)
    {
        uint16 chunk_usage = entry->chunk_usage[chunkno];
        BlockRefTableChunk chunk_data = entry->chunk_data[chunkno];
        unsigned start_offset = 0;
        unsigned stop_offset = BLOCKS_PER_CHUNK;

        // Adjust offsets for partial chunks at range boundaries
        if (chunkno == start_chunkno)
            start_offset = start_blkno % BLOCKS_PER_CHUNK;
        if (chunkno == stop_chunkno - 1)
        {
            Assert(stop_blkno > chunkno * BLOCKS_PER_CHUNK);
            stop_offset = stop_blkno - (chunkno * BLOCKS_PER_CHUNK);
            Assert(stop_offset <= BLOCKS_PER_CHUNK);
        }

        if (chunk_usage == MAX_ENTRIES_PER_CHUNK)
        {
            // Bitmap format: test each bit in range
            for (unsigned i = start_offset; i < stop_offset; ++i)
            {
                uint16 w = chunk_data[i / BLOCKS_PER_ENTRY];

                if ((w & (1 << (i % BLOCKS_PER_ENTRY))) != 0)
                {
                    BlockNumber blkno = chunkno * BLOCKS_PER_CHUNK + i;
                    blocks[nresults++] = blkno;

                    // Exit early if output buffer is full
                    if (nresults == nblocks)
                        return nresults;
                }
            }
        }
        else
        {
            // Offset array format: check each stored offset
            for (unsigned i = 0; i < chunk_usage; ++i)
            {
                uint16 offset = chunk_data[i];

                if (offset >= start_offset && offset < stop_offset)
                {
                    BlockNumber blkno = chunkno * BLOCKS_PER_CHUNK + offset;
                    blocks[nresults++] = blkno;

                    // Exit early if output buffer is full
                    if (nresults == nblocks)
                        return nresults;
                }
            }
        }
    }

    return nresults;
}
```