# TidStoreSetBlockOffsets

## Location
[src/backend/access/common/tidstore.c:356-431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tidstore.c#L356-L431)

## Overview
Creates or replaces an entry in the TidStore for a given block number and array of offset numbers, optimized for vacuum operations.

## Definition
```c
void TidStoreSetBlockOffsets(TidStore *ts, BlockNumber blkno, OffsetNumber *offsets, int num_offsets)
```

## Detailed Description
TidStoreSetBlockOffsets creates or replaces an entry in the TidStore for the specified block number with the provided array of offset numbers. The function is specifically designed and optimized for vacuum's heap scanning phase. It supports two storage modes based on the number of offsets: for small numbers (≤ NUM_FULL_OFFSETS), it stores offsets directly in the header; for larger numbers, it uses a bitmap representation. The function validates that offset numbers are in ascending order and within valid bounds, then stores the data in either the shared or local radix tree depending on the TidStore configuration.

## Parameters / Member Variables
- `ts`: Pointer to the TidStore object
- `blkno`: Block number for which to set the offsets
- `offsets`: Array of offset numbers, must be sorted in ascending order
- `num_offsets`: Number of offsets in the array (must be > 0)

## Dependencies
- Functions called/Symbols referenced:
  - TidStoreIsShared (macro)
  - shared_ts_set (radix tree generated function)
  - local_ts_set (radix tree generated function)
  - MaxBlocktableEntrySize
  - [BlocktableEntry](../B/BlocktableEntry.md)
  - NUM_FULL_OFFSETS
  - BITS_PER_BITMAPWORD
  - WORDNUM, BITNUM, WORDS_PER_PAGE (macros)
  - InvalidOffsetNumber, MAX_OFFSET_IN_BITMAP
- Called from (representative examples):
  - [dead_items_add](../d/dead_items_add.md) (in vacuumlazy.c)
  - [do_set_block_offsets](../d/do_set_block_offsets.md) (in test_tidstore.c)

## Notes and Other Information
- The offset numbers must be sorted in ascending order
- If the block number already exists, the entry will be completely replaced (no way to add/remove individual offsets)
- Designed and optimized for vacuum's heap scanning phase
- Uses two storage strategies: direct storage for few offsets, bitmap for many offsets
- Performs bounds checking on offset numbers to prevent array overruns
- Stores data in shared or local radix tree based on TidStore configuration
- Errors if offset numbers are invalid or out of range

## Simplified Source

```c
void TidStoreSetBlockOffsets(TidStore *ts, BlockNumber blkno,
                           OffsetNumber *offsets, int num_offsets)
{
    union {
        char data[MaxBlocktableEntrySize];
        BlocktableEntry force_align_entry;
    } data;
    BlocktableEntry *page = (BlocktableEntry *) data.data;

    Assert(num_offsets > 0);

    // Validate offsets are sorted
    for (int i = 1; i < num_offsets; i++)
        Assert(offsets[i] > offsets[i - 1]);

    memset(page, 0, offsetof(BlocktableEntry, words));

    if (num_offsets <= NUM_FULL_OFFSETS) {
        // Small number of offsets: store directly in header
        for (int i = 0; i < num_offsets; i++) {
            OffsetNumber off = offsets[i];

            if (off == InvalidOffsetNumber || off > MAX_OFFSET_IN_BITMAP)
                elog(ERROR, "tuple offset out of range: %u", off);

            page->header.full_offsets[i] = off;
        }
        page->header.nwords = 0;
    } else {
        // Large number of offsets: use bitmap representation
        int idx = 0;
        for (int wordnum = 0; wordnum <= WORDNUM(offsets[num_offsets - 1]); wordnum++) {
            bitmapword word = 0;
            int next_word_threshold = (wordnum + 1) * BITS_PER_BITMAPWORD;

            // Set bits for offsets in this word
            while (idx < num_offsets && offsets[idx] < next_word_threshold) {
                OffsetNumber off = offsets[idx];

                if (off == InvalidOffsetNumber || off > MAX_OFFSET_IN_BITMAP)
                    elog(ERROR, "tuple offset out of range: %u", off);

                word |= ((bitmapword) 1 << BITNUM(off));
                idx++;
            }

            page->words[wordnum] = word;
        }

        page->header.nwords = WORDS_PER_PAGE(offsets[num_offsets - 1]);
    }

    // Store in appropriate radix tree
    if (TidStoreIsShared(ts))
        shared_ts_set(ts->tree.shared, blkno, page);
    else
        local_ts_set(ts->tree.local, blkno, page);
}
```