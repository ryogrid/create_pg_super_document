# RT_EXTEND_UP

## Location
src/include/lib/radixtree.h: 1581 - 1615

## Overview
A macro that generates the function name for extending a radix tree upward by adding new root levels when inserting keys that exceed the current tree height capacity.

## Definition
```c
#define RT_EXTEND_UP RT_MAKE_NAME(extend_up)
static pg_noinline void RT_EXTEND_UP(RT_RADIX_TREE *tree, uint64 key)
```

## Detailed Description
RT_EXTEND_UP is both a macro that expands to a function name and a critical function responsible for growing the radix tree vertically when a key requires more tree levels than currently exist. This operation is necessary when inserting keys with higher-order bits that cannot be accommodated by the current tree height.

The function calculates the required shift level for the new key and iteratively adds new root levels by creating node-4 structures. Each new level increases the tree's capacity to handle larger key values. The process continues until the tree has sufficient height to accommodate the target key.

The function is marked as `pg_noinline` since tree extension is a relatively rare operation compared to normal insertions, and inlining would increase code size for the common case without significant benefit.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure that needs to be extended
- `key`: The 64-bit key value that requires additional tree height

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - RT_KEY_GET_SHIFT (calculates required shift level for the key)
  - RT_ALLOC_NODE (allocates new node-4 structures)
  - RT_SHIFT_GET_MAX_VAL (calculates maximum value for given shift)
  - RT_SPAN (defines bits per tree level - 8 bits)
- Called from (representative examples):
  - RT_SET (when tree height is insufficient for the key being inserted)

## Notes and Other Information
- Critical for handling keys that exceed the current tree capacity
- Creates new node-4 structures as intermediate levels since they are the most memory-efficient
- Updates both the root pointer and the tree control structure (start_shift, max_val)
- Each iteration adds RT_SPAN (8 bits) to the tree's capacity
- The old tree becomes a subtree under the new root structure
- Maintains tree integrity by properly linking the existing tree as the first child of new root levels
- Relatively infrequent operation that enables the radix tree to handle arbitrary 64-bit key ranges
- Part of the adaptive radix tree's ability to handle sparse key distributions efficiently