# RT_ITER

## Location
src/include/lib/radixtree.h: 746 - 766

## Overview
RT_ITER is a macro that expands to a type name for the main iterator structure used to traverse PostgreSQL's radix tree data structure.

## Definition
`#define RT_ITER RT_MAKE_NAME(iter)`

This expands to a typedef name based on the RT_PREFIX configuration, typically resulting in a structure name like `<prefix>_iter`.

## Detailed Description
RT_ITER is the primary iteration interface for PostgreSQL's generic radix tree implementation. It provides a complete traversal mechanism that can iterate through all key-value pairs in the radix tree in sorted order. The iterator maintains a stack of node iterators to handle the multi-level nature of the radix tree structure.

The structure it represents contains:
- A pointer to the radix tree being iterated
- A stack of RT_NODE_ITER structures for tracking position at each tree level
- Level tracking variables for managing the traversal stack
- The current key being constructed during iteration

## Parameters / Member Variables
- `tree`: Pointer to the RT_RADIX_TREE being iterated over
- `node_iters[RT_MAX_LEVEL]`: Stack of RT_NODE_ITER structures, one for each tree level (level 0 is the leaf level)
- `top_level`: The highest level currently in use in the iteration stack
- `cur_level`: The current level being processed during iteration
- `key`: The 64-bit key value constructed during the current iteration step

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro expansion system)
  - RT_NODE_ITER (component type for the node iterator stack)
  - RT_RADIX_TREE (the tree type being iterated)
- Called from (representative examples):
  - RT_BEGIN_ITERATE (initializes RT_ITER)
  - RT_ITERATE_NEXT (advances RT_ITER)
  - RT_END_ITERATE (cleans up RT_ITER)
  - RT_FREE (may reference RT_ITER)

## Notes and Other Information
- Part of PostgreSQL's templated radix tree system enabling multiple type-safe instances
- Supports resumable iteration - can pause and continue traversal at any point
- Iteration order follows the natural key ordering (ascending)
- The iterator stack design efficiently handles trees of varying depths up to RT_MAX_LEVEL
- Essential for bulk operations and range queries on radix tree data
- Used in both shared memory and local memory variants of the radix tree