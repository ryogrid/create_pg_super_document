# datapagemap_add

## Location
[src/bin/pg_rewind/datapagemap.c:32-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/datapagemap.c#L32-L74)

## Overview
Adds a specific block number to a bitmap data structure used for tracking modified data pages in the pg_rewind utility.

## Definition
void datapagemap_add(datapagemap_t *map, BlockNumber blkno)

## Detailed Description
This function sets the bit corresponding to a specific block number in the datapagemap bitmap. The bitmap uses a simple bit-per-block approach where each bit represents whether a corresponding data page has been modified. The function dynamically expands the bitmap size as needed when adding blocks that exceed the current bitmap capacity. When expanding, it allocates additional headroom (10 extra bytes) to minimize the need for repeated reallocations when blocks are added sequentially.

## Parameters / Member Variables
- : Pointer to the datapagemap_t structure containing the bitmap and its metadata
- : The block number to be marked as modified in the bitmap

## Dependencies
- Functions called/Symbols referenced:
  - [pg_realloc](../p/pg_realloc.md) (for expanding the bitmap when needed)
  - memset (for zeroing newly allocated bitmap regions)
- Called from (representative examples):
  - [process_target_wal_block_change](../p/process_target_wal_block_change.md) (in filemap.c:399)

## Notes and Other Information
- The bitmap is byte-oriented with 8 bits per byte, using simple modulo arithmetic to determine byte offset and bit position
- Dynamic allocation strategy includes 10-byte headroom to optimize for sequential block additions
- Part of the pg_rewind utility's data page tracking system for PostgreSQL database synchronization

## Simplified Source

```c
void datapagemap_add(datapagemap_t *map, BlockNumber blkno)
{
    int offset;
    int bitno;

    // Calculate byte offset and bit position
    offset = blkno / 8;
    bitno = blkno % 8;

    // Expand bitmap if needed
    if (map->bitmapsize <= offset)
    {
        int oldsize = map->bitmapsize;
        int newsize = offset + 1 + 10; // Add headroom for sequential access

        // Reallocate and zero new region
        map->bitmap = pg_realloc(map->bitmap, newsize);
        memset(&map->bitmap[oldsize], 0, newsize - oldsize);
        map->bitmapsize = newsize;
    }

    // Set the bit for this block
    map->bitmap[offset] |= (1 << bitno);
}
```