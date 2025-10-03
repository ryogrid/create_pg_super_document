# mask_lp_flags

## Location
[src/backend/access/common/bufmask.c:95-118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/bufmask.c#L95-L118)

## Overview
Masks line pointer flags that can be modified on the primary server without generating WAL records, ensuring consistent page comparisons for index access methods.

## Definition
```c
void mask_lp_flags(Page page)
```

## Detailed Description
This function masks the lp_flags field in line pointers (ItemId structures) on a page. In some index access methods, these flags can be modified on the primary server without emitting corresponding WAL records. Since WAL replay would not recreate these flag changes, the flags must be masked out during consistency checks to prevent false mismatches.

The function iterates through all line pointers on the page and sets the lp_flags field to LP_UNUSED for any item pointers that are currently in use. This ensures that flag differences between the original page and the WAL-replayed page don't cause consistency check failures.

## Parameters / Member Variables
- `page`: A pointer to the page whose line pointer flags should be masked

## Dependencies
- Functions called/Symbols referenced:
  - OffsetNumber (type for line pointer offsets)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md) (macro to get highest offset number on page)
  - FirstOffsetNumber (constant for first valid offset number)
  - OffsetNumberNext (macro to increment offset number)
  - [PageGetItemId](../P/PageGetItemId.md) (macro to get ItemId by offset)
  - ItemId (type for line pointer structure)
  - ItemIdIsUsed (macro to check if line pointer is in use)
  - LP_UNUSED (constant flag value for unused line pointers)
- Called from (representative examples):
  - [gist_mask](../g/gist_mask.md) (GiST index masking)
  - [hash_mask](../h/hash_mask.md) (hash index masking)
  - [btree_mask](../b/btree_mask.md) (B-tree index masking)

## Notes and Other Information
- This function is specifically used for index pages where line pointer flags can change asynchronously
- Not all access methods require this masking - only those that can modify lp_flags without WAL logging
- The function only processes line pointers that are currently marked as used
- LP_UNUSED is used as the mask value to normalize all active line pointer flags
- Line pointers are fundamental structures in PostgreSQL pages that track the location and status of items
- This masking is essential for WAL consistency verification in index access methods that use hint-bit-like optimizations in line pointers

## Simplified Source

```c
void mask_lp_flags(Page page)
{
    OffsetNumber max_offset = PageGetMaxOffsetNumber(page);

    // Iterate through all line pointers on the page
    for (OffsetNumber offset = FirstOffsetNumber;
         offset <= max_offset;
         offset = OffsetNumberNext(offset))
    {
        ItemId line_pointer = PageGetItemId(page, offset);

        // Mask flags for line pointers that are in use
        if (ItemIdIsUsed(line_pointer))
            line_pointer->lp_flags = LP_UNUSED;
    }
}
```