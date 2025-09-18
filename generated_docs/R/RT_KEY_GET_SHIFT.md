# RT_KEY_GET_SHIFT

## Location
src/include/lib/radixtree.h: 810 - 821

## Overview
A macro that expands to a function calculating the smallest shift value required to accommodate a given key in a radix tree structure.

## Definition
```c
#define RT_KEY_GET_SHIFT RT_MAKE_NAME(key_get_shift)

static inline int
RT_KEY_GET_SHIFT(uint64 key)
{
    if (key == 0)
        return 0;
    else
        return (pg_leftmost_one_pos64(key) / RT_SPAN) * RT_SPAN;
}
```

## Detailed Description
This function determines the minimum shift value needed to store a given key in the radix tree by finding the position of the leftmost (most significant) set bit in the key. The shift value represents how many bits to shift right when extracting chunks from the key during tree traversal.

The function works by:
1. Handling the special case where key is 0, returning shift 0
2. For non-zero keys, finding the position of the leftmost set bit using pg_leftmost_one_pos64()
3. Dividing by RT_SPAN (8 bits) and multiplying back to get a shift aligned to byte boundaries
4. This ensures the shift is always a multiple of RT_SPAN (8), aligning with the tree's byte-oriented chunking

## Parameters / Member Variables
- `key`: A 64-bit unsigned integer representing the key to be stored in the radix tree

## Dependencies
- Functions called/Symbols referenced:
  - [pg_leftmost_one_pos64](../p/pg_leftmost_one_pos64.md) (PostgreSQL bit manipulation function)
  - RT_SPAN (constant defining bits per tree level, equals BITS_PER_BYTE = 8)
  - RT_MAKE_NAME (macro name generation)
- Called from (representative examples):
  - RT_MAX_SHIFT (for calculating maximum possible shift)
  - [RT_EXTEND_UP](RT_EXTEND_UP.md) (when growing tree upward to accommodate larger keys)
  - [RT_SET](RT_SET.md) (during key insertion operations)

## Notes and Other Information
- The shift value determines the tree height needed to store the key efficiently
- RT_SPAN is defined as BITS_PER_BYTE (8), meaning each tree level processes 8 bits (1 byte) of the key
- The alignment to RT_SPAN boundaries ensures consistent byte-oriented key chunking throughout the tree
- This function is critical for dynamic tree growth - trees start small and expand upward as needed
- Used in PostgreSQL's generic radix tree implementation for efficient sparse key-value storage
- The returned shift is always a multiple of 8, corresponding to byte boundaries in the 64-bit key