# RT_FREE_LEAF

## Location
src/include/lib/radixtree.h: 959 - 982

## Overview
RT_FREE_LEAF is a macro that resolves to a function for deallocating radix tree leaf nodes and updating debug statistics.

## Definition
```c
#define RT_FREE_LEAF RT_MAKE_NAME(free_leaf)

// The actual function signature:
static inline void RT_FREE_LEAF(RT_RADIX_TREE * tree, RT_PTR_ALLOC leaf)
```

## Detailed Description
RT_FREE_LEAF is part of PostgreSQL's templated radix tree implementation that handles the deallocation of leaf nodes. The function includes safety checks to prevent freeing the root node and updates debug statistics when RT_DEBUG is enabled. Like other memory management functions in the radix tree, it supports both shared memory (RT_SHMEM) and regular memory context allocation strategies. The function is marked inline for performance since leaf deallocation can be frequent during tree operations.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure that contains the leaf to be freed
- `leaf`: RT_PTR_ALLOC representing the leaf node to be deallocated (raw allocation pointer)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for name generation)
  - [dsa_free](../d/dsa_free.md) (for shared memory deallocation when RT_SHMEM is defined)
  - [pfree](../p/pfree.md) (for regular memory deallocation)
  - Assert (for debug assertions)
- Called from (representative examples):
  - [RT_SET](RT_SET.md) (when replacing existing leaf values)
  - [RT_DELETE_RECURSIVE](RT_DELETE_RECURSIVE.md) (during node deletion operations)

## Notes and Other Information
- Includes an assertion to prevent accidentally freeing the root node
- Updates debug statistics by decrementing the leaf count when RT_DEBUG is enabled
- Uses inline function optimization for performance during frequent leaf operations
- Memory deallocation method depends on RT_SHMEM compilation flag (shared vs local memory)
- Takes RT_PTR_ALLOC parameter (raw allocation pointer) rather than RT_CHILD_PTR structure
- Essential for preventing memory leaks during value updates and tree deletions
- The function assumes the caller has already verified that the leaf is safe to free
- Does not perform any cleanup of the leaf's content - caller must handle value-specific cleanup if needed