# bms_singleton_member

## Location
[src/backend/nodes/bitmapset.c:672-714](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L672-L714)

## Overview
Returns the sole integer member of a bitmapset, throwing an error if the set does not contain exactly one member.

## Definition
```c
int bms_singleton_member(const Bitmapset *a)
```

## Detailed Description
This function extracts the single member from a bitmapset that is expected to contain exactly one element. It validates that the bitmapset contains exactly one member and returns that member's integer value. The function scans through each word in the bitmapset, checking for non-zero values. If it finds more than one word with bits set, or if any word contains multiple bits, it raises an error. When it finds the single set bit, it calculates the member's integer value by combining the word position with the bit position within that word.

## Parameters / Member Variables
- `a`: The bitmapset that must contain exactly one member (const Bitmapset *)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md)
  - bitmapword
  - HAS_MULTIPLE_ONES
  - BITS_PER_BITMAPWORD
  - bmw_rightmost_one_pos
- Called from (representative examples):
  - [get_matching_part_pairs](../g/get_matching_part_pairs.md) (src/backend/optimizer/path/joinrels.c:1934, 1955)
  - [remove_useless_joins](../r/remove_useless_joins.md) (src/backend/optimizer/plan/analyzejoins.c:88)
  - [fix_append_rel_relids](../f/fix_append_rel_relids.md) (src/backend/optimizer/prep/prepjointree.c:4058)

## Notes and Other Information
- Throws ERROR if the bitmapset is NULL (empty)
- Throws ERROR if the bitmapset contains multiple members
- Uses efficient bit manipulation to detect multiple bits in a single word via HAS_MULTIPLE_ONES macro
- Calculates the final member value by combining word offset (wordnum * BITS_PER_BITMAPWORD) with bit position
- Commonly used in query optimization when a singleton set is expected
- Located in src/backend/nodes/bitmapset.c:672-714

## Simplified Source

```c
int bms_singleton_member(const Bitmapset *a)
{
    int result = -1;
    int wordnum = 0;

    // Validate input
    if (a == NULL)
        elog(ERROR, "bitmapset is empty");

    // Scan each word for set bits
    do {
        bitmapword w = a->words[wordnum];

        if (w != 0) {
            // Check for multiple members (either multiple words or multiple bits in one word)
            if (result >= 0 || HAS_MULTIPLE_ONES(w))
                elog(ERROR, "bitmapset has multiple members");

            // Calculate member value: word offset + bit position
            result = wordnum * BITS_PER_BITMAPWORD;
            result += bmw_rightmost_one_pos(w);
        }
    } while (++wordnum < a->nwords);

    return result;
}
```