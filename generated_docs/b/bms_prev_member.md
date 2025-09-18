# bms_prev_member

## Location
src/backend/nodes/bitmapset.c: 1367 - 1415

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
  - bms_is_valid_set
  - bmw_leftmost_one_pos
  - WORDNUM (macro)
  - BITNUM (macro)  
  - BITS_PER_BITMAPWORD (constant)
  - bitmapword (type)
- Called from (examples):
  - choose_next_subplan_locally
  - bms_is_empty (header usage)

## Notes and Other Information
- Returns -2 (not -1) when no more members exist to distinguish loop-completed state from loop-not-started state
- Typical usage pattern: `x = -1; while ((x = bms_prev_member(set, x)) >= 0) process(x);`
- The `prevbit` parameter must not exceed one above the highest possible bit in the current Bitmapset size
- Handles NULL input gracefully by returning -2
- Complements `bms_next_member` for forward iteration