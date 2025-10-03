# bms_prev_member

## Location
[src/backend/nodes/bitmapset.c:1367-1415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L1367-L1415)

## Overview
The `bms_prev_member` function finds the previous (largest) member less than a specified bit position in a Bitmapset, supporting reverse iteration through set members.

## Definition
```c
int bms_prev_member(const Bitmapset *a, int prevbit)
```

## Detailed Description
This function is designed to support iterating through the members of a Bitmapset in reverse order (from highest to lowest bit position). It returns the largest member that is less than the given `prevbit` parameter, or -2 if no such member exists.

The function includes special handling for the initial state of reverse iteration: when `prevbit` is -1, it finds the highest valued member in the set. This allows for a clean iteration pattern where the caller starts with -1 and continues until the function returns -2.

The implementation works by:
1. Converting the prevbit to the actual bit position to search from
2. Creating a mask to exclude bits at or above the search position  
3. Iterating through bitmap words from high to low
4. Using `bmw_leftmost_one_pos` to find the highest set bit in each word
5. Returning the absolute bit position when found

## Parameters
- `a`: Pointer to the Bitmapset to search (can be NULL)
- `prevbit`: Starting bit position for the search, or -1 to find the highest member

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md)
  - bmw_leftmost_one_pos
  - WORDNUM (macro)
  - BITNUM (macro)  
  - BITS_PER_BITMAPWORD (constant)
  - bitmapword (type)
- Called from (examples):
  - [choose_next_subplan_locally](../c/choose_next_subplan_locally.md)
  - bms_is_empty (header usage)

## Notes and Other Information
- Returns -2 (not -1) when no more members exist to distinguish loop-completed state from loop-not-started state
- Typical usage pattern: `x = -1; while ((x = bms_prev_member(set, x)) >= 0) process(x);`
- The `prevbit` parameter must not exceed one above the highest possible bit in the current Bitmapset size
- Handles NULL input gracefully by returning -2
- Complements `bms_next_member` for forward iteration

## Simplified Source

```c
int
bms_prev_member(const Bitmapset *a, int prevbit)
{
    // Handle NULL set or edge case
    if (a == NULL || prevbit == 0)
        return -2;

    // Special case: -1 means find highest member in set
    if (prevbit == -1)
        prevbit = a->nwords * BITS_PER_BITMAPWORD - 1;
    else
        prevbit--; // Move to actual search position

    // Calculate mask to exclude bits >= prevbit
    int ushiftbits = BITS_PER_BITMAPWORD - (BITNUM(prevbit) + 1);
    bitmapword mask = (~(bitmapword) 0) >> ushiftbits;

    // Search through words from high to low
    for (int wordnum = WORDNUM(prevbit); wordnum >= 0; wordnum--)
    {
        bitmapword w = a->words[wordnum];

        // Apply mask to exclude unwanted bits
        w &= mask;

        if (w != 0)
        {
            // Found a set bit - calculate absolute position
            int result = wordnum * BITS_PER_BITMAPWORD;
            result += bmw_leftmost_one_pos(w);
            return result;
        }

        // For subsequent words, check all bits
        mask = (~(bitmapword) 0);
    }

    return -2; // No previous member found
}
```