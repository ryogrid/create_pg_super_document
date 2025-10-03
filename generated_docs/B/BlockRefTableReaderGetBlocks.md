# BlockRefTableReaderGetBlocks

## Location
[src/common/blkreftable.c:689-772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L689-L772)

## Overview
Extracts modified block numbers from the current relation fork in a block reference table, supporting both bitmap and offset list formats for efficient block reference retrieval.

## Definition

```c
unsigned
BlockRefTableReaderGetBlocks(BlockRefTableReader *reader,
							 BlockNumber *blocks,
							 int nblocks)
```
## Detailed Description
BlockRefTableReaderGetBlocks retrieves block numbers of modified blocks from the relation fork currently selected by the most recent call to BlockRefTableReaderNextRelation. The function handles two different data formats: bitmap format (when chunk size equals MAX_ENTRIES_PER_CHUNK) where each bit represents a block, and offset list format where each entry is a 2-byte block offset. It processes chunks sequentially, reading chunk data on demand and maintaining position within the current chunk. The function continues until either the requested number of blocks is found or all chunks for the current relation are exhausted.

## Parameters / Member Variables
- `*reader`: Pointer to the BlockRefTableReader containing the current read state and chunk information
- `*blocks`: Output array where block numbers will be written
- `nblocks`: Maximum number of block numbers that can be stored in the blocks array
## Dependencies
- Functions called/Symbols referenced:
  - [BlockRefTableRead](BlockRefTableRead.md)
  - MAX_ENTRIES_PER_CHUNK
  - BLOCKS_PER_CHUNK
  - BLOCKS_PER_ENTRY
- Called from (representative examples):
  - [PrepareForIncrementalBackup](../P/PrepareForIncrementalBackup.md)
  - [pg_wal_summary_contents](../p/pg_wal_summary_contents.md)
  - [dump_one_relation](../d/dump_one_relation.md)

## Notes and Other Information
- Returns the actual number of block numbers written to the blocks array (may be less than nblocks)
- Supports two chunk formats: bitmap (bit per block) and offset list (2-byte offsets)
- Bitmap format is used when chunk size equals MAX_ENTRIES_PER_CHUNK for dense block references
- Offset list format stores explicit block offsets within the chunk for sparse references
- Maintains internal state (chunk_position, consumed_chunks) to support incremental reading
- Automatically advances to next chunk when current chunk is exhausted
- Handles empty chunks by consuming them without reading data from the underlying file
- Must be called repeatedly until it returns 0 before advancing to the next relation

## Simplified Source

```c
unsigned
BlockRefTableReaderGetBlocks(BlockRefTableReader *reader,
                            BlockNumber *blocks,
                            int nblocks)
{
    unsigned blocks_found = 0;

    Assert(nblocks > 0);

    // Loop collecting blocks to return to caller
    for (;;)
    {
        uint16 next_chunk_size;

        // Process current chunk if we have one
        if (reader->consumed_chunks > 0)
        {
            uint32 chunkno = reader->consumed_chunks - 1;
            uint16 chunk_size = reader->chunk_size[chunkno];

            if (chunk_size == MAX_ENTRIES_PER_CHUNK)
            {
                // Bitmap format: search for set bits
                while (reader->chunk_position < BLOCKS_PER_CHUNK &&
                       blocks_found < nblocks)
                {
                    uint16 chunkoffset = reader->chunk_position;
                    uint16 w;

                    w = reader->chunk_data[chunkoffset / BLOCKS_PER_ENTRY];
                    if ((w & (1u << (chunkoffset % BLOCKS_PER_ENTRY))) != 0)
                        blocks[blocks_found++] =
                            chunkno * BLOCKS_PER_CHUNK + chunkoffset;
                    ++reader->chunk_position;
                }
            }
            else
            {
                // Offset list format: each entry is a 2-byte offset
                while (reader->chunk_position < chunk_size &&
                       blocks_found < nblocks)
                {
                    blocks[blocks_found++] = chunkno * BLOCKS_PER_CHUNK
                        + reader->chunk_data[reader->chunk_position];
                    ++reader->chunk_position;
                }
            }
        }

        // Return if we found enough blocks
        if (blocks_found >= nblocks)
            break;

        // Break if no more chunks available
        if (reader->consumed_chunks == reader->total_chunks)
            break;

        // Read next chunk and reset position
        next_chunk_size = reader->chunk_size[reader->consumed_chunks];
        if (next_chunk_size > 0)
            BlockRefTableRead(reader, reader->chunk_data,
                             next_chunk_size * sizeof(uint16));
        ++reader->consumed_chunks;
        reader->chunk_position = 0;
    }

    return blocks_found;
}
```