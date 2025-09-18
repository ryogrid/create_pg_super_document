# RT_REMOVE_CHILD_16

## Location
[src/include/lib/radixtree.h:2498-2522](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L2498-L2522)

## Overview
RT_REMOVE_CHILD_16 is a macro that expands to a function responsible for removing a child entry from a node16 in PostgreSQL's radix tree implementation.

## Definition
```c
#define RT_REMOVE_CHILD_16 RT_MAKE_NAME(remove_child_16)

static inline void
RT_REMOVE_CHILD_16(RT_RADIX_TREE *tree, RT_PTR_ALLOC *parent_slot, RT_CHILD_PTR node, uint8 chunk, RT_PTR_ALLOC *slot)
```

## Detailed Description
This function removes a child entry from a node16 structure in the radix tree. A node16 is an intermediate node type that can hold up to 16 children and stores them in sorted arrays (chunks and children) for efficient access. The function uses the provided slot pointer to determine the exact position to delete and handles two scenarios:

1. **Node Shrinking**: If the node16 has 4 or fewer entries after deletion, it triggers shrinking to a node4 for better performance, as linear search is faster than SIMD for small counts
2. **In-place Deletion**: If the node remains viable as node16, it performs in-place deletion by shifting the arrays to remove the specified entry

The function includes an important optimization where the shrinking threshold (4) is carefully chosen based on performance characteristics - linear search outperforms SIMD for small element counts on x86-64 architecture.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure
- `parent_slot`: Pointer to the parent's slot that contains this node (for potential node replacement)
- `node`: The node16 from which to remove the child
- `chunk`: The 8-bit chunk value identifying which child to remove
- `slot`: Pointer to the exact slot in the children array to be removed (used to calculate deletepos)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - [RT_SHRINK_NODE_16](RT_SHRINK_NODE_16.md)
  - [RT_SHIFT_ARRAYS_AND_DELETE](RT_SHIFT_ARRAYS_AND_DELETE.md)
- Called from (representative examples):
  - [RT_NODE_DELETE](RT_NODE_DELETE.md)

## Notes and Other Information
- This function is marked as static inline for performance optimization
- The shrinking threshold of 4 is hard-coded based on performance analysis showing linear search is faster than SIMD for ≤3 elements
- Uses the slot parameter (not available in other remove_child functions) to efficiently calculate the deletion position
- Part of PostgreSQL's generic radix tree implementation that uses C macros for type polymorphism
- Node16 stores children in sorted arrays, making SIMD-optimized searches possible for larger counts
- Located in src/include/lib/radixtree.h at lines 2498-2520