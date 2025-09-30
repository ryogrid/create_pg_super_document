# bms_add_range

## Location
[src/backend/nodes/bitmapset.c:1019-1108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L1019-L1108)

## Overview
Efficiently adds a contiguous range of members from 'lower' to 'upper' (inclusive) to a bitmap set, working at the bitmapword level for performance.

## Definition

```c
Bitmapset *
bms_add_range(Bitmapset *a, int lower, int upper)
```
## Detailed Description
The bms_add_range function adds all integer members in the specified range [lower, upper] to the bitmap set. This function is optimized for adding large ranges of consecutive members by operating at the bitmapword level rather than setting individual bits. It handles memory allocation automatically, expanding the bitmap set if necessary to accommodate the upper bound.

The function performs several optimizations:
- Early return if upper < lower (empty range)
- Efficient word-level operations for setting multiple bits
- Special handling when the range spans a single word vs multiple words
- Automatic memory management with reallocation when needed

## Parameters / Member Variables
- : The input bitmap set to modify (can be NULL for creating a new set)
- : The lowest member value to add to the set (must be non-negative)
- : The highest member value to add to the set (inclusive)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md) (validation)
  - [bms_copy_and_free](bms_copy_and_free.md) (memory management)
  - WORDNUM (bit position to word index conversion)
  - BITNUM (bit position within word calculation)
  - BITMAPSET_SIZE (memory size calculation)
  - [repalloc](../r/repalloc.md) (memory reallocation)
  - BITS_PER_BITMAPWORD (word size constant)
  
- Called from (representative examples):
  - [ExecInitPartitionPruning](../E/ExecInitPartitionPruning.md) (partition pruning initialization)
  - [ExecInitAppend](../E/ExecInitAppend.md)/ExecInitMergeAppend (append node initialization)
  - [get_matching_partitions](../g/get_matching_partitions.md) (partition matching logic)
  - [prune_append_rel_partitions](../p/prune_append_rel_partitions.md) (partition pruning)

## Notes and Other Information
- Raises an ERROR if lower < 0 (negative bitmap members not allowed)
- More efficient than calling bms_add_member in a loop for large ranges
- Automatically handles memory allocation and reallocation
- Uses bit manipulation techniques for optimal performance
- Supports conditional reallocation based on REALLOCATE_BITMAPSETS compile flag
- The function is extensively used in PostgreSQL's partition pruning system

## Simplified Source

```c
Bitmapset *
bms_add_range(Bitmapset *a, int lower, int upper)
{
    int lwordnum, lbitnum, uwordnum, ushiftbits, wordnum;

    Assert(bms_is_valid_set(a));

    // Early return for empty range
    if (upper < lower)
        return a;

    // Validate lower bound
    if (lower < 0)
        elog(ERROR, "negative bitmapset member not allowed");

    uwordnum = WORDNUM(upper);

    // Allocate or expand bitmapset as needed
    if (a == NULL)
    {
        a = (Bitmapset *) palloc0(BITMAPSET_SIZE(uwordnum + 1));
        a->type = T_Bitmapset;
        a->nwords = uwordnum + 1;
    }
    else if (uwordnum >= a->nwords)
    {
        // Expand to accommodate upper bound
        int oldnwords = a->nwords;
        a = (Bitmapset *) repalloc(a, BITMAPSET_SIZE(uwordnum + 1));
        a->nwords = uwordnum + 1;

        // Zero out new words
        for (int i = oldnwords; i < a->nwords; i++)
            a->words[i] = 0;
    }

    lwordnum = WORDNUM(lower);
    lbitnum = BITNUM(lower);
    ushiftbits = BITS_PER_BITMAPWORD - (BITNUM(upper) + 1);

    // Set bits efficiently at word level
    if (lwordnum == uwordnum)
    {
        // Range within single word - apply both masks
        a->words[lwordnum] |= ~((bitmapword) ((1 << lbitnum) - 1)) &
                              (~(bitmapword) 0) >> ushiftbits;
    }
    else
    {
        // Range spans multiple words
        wordnum = lwordnum;

        // Set lower partial word
        a->words[wordnum++] |= ~((bitmapword) ((1 << lbitnum) - 1));

        // Set all intermediate words
        while (wordnum < uwordnum)
            a->words[wordnum++] = ~(bitmapword) 0;

        // Set upper partial word
        a->words[uwordnum] |= (~(bitmapword) 0) >> ushiftbits;
    }

    return a;
}
```