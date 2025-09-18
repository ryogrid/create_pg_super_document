# RT_SHIFT_GET_MAX_VAL

## Location
src/include/lib/radixtree.h: 822 - 833

## Overview
A function that calculates the maximum key value that can be stored in a radix tree with a given shift configuration.

## Definition
```c
#define RT_SHIFT_GET_MAX_VAL RT_MAKE_NAME(shift_get_max_val)

static uint64
RT_SHIFT_GET_MAX_VAL(int shift)
{
    if (shift == RT_MAX_SHIFT)
        return UINT64_MAX;
    else
        return (UINT64CONST(1) << (shift + RT_SPAN)) - 1;
}
```

## Detailed Description
This function determines the maximum key value that can be accommodated by a radix tree configured with a specific shift value. The shift determines the tree height and the range of keys that can be stored without requiring tree expansion.

The function works by:
1. Handling the special case where shift equals RT_MAX_SHIFT (maximum possible shift), returning UINT64_MAX
2. For other shift values, calculating 2^(shift + RT_SPAN) - 1, which represents the largest key that can be encoded at that tree height
3. The "+ RT_SPAN" accounts for the additional level that the current shift can address

The result defines the upper bound of keys that the tree can store without needing to expand upward.

## Parameters / Member Variables
- `shift`: An integer representing the current shift configuration of the radix tree (must be a multiple of RT_SPAN)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAX_SHIFT (maximum possible shift value)
  - RT_SPAN (bits per tree level, equals BITS_PER_BYTE = 8)
  - UINT64CONST (PostgreSQL macro for 64-bit constants)
  - RT_MAKE_NAME (macro name generation)
- Called from (representative examples):
  - RT_EXTEND_UP (when updating tree capacity after expansion)
  - RT_SET (during key insertion to check capacity)
  - RT_CREATE (when initializing new trees)
  - RT_REMOVE_CHILD_4 (during tree shrinking operations)

## Notes and Other Information
- The returned value represents the inclusive upper bound of keys that can be stored
- When shift equals RT_MAX_SHIFT, the tree can store any 64-bit key value
- This function is critical for determining when tree expansion is needed during insertion
- Used in PostgreSQL's radix tree implementation to maintain optimal tree height
- The calculation accounts for the tree's byte-oriented chunking (RT_SPAN = 8 bits)
- Tree capacity grows exponentially with each level: each RT_SPAN increase multiplies capacity by 256