# RT_REMOVE_CHILD_256

## Location
src/include/lib/radixtree.h: 2373 - 2403

## Overview
A macro that defines a function name for removing a child pointer from a node256 in the radix tree and conditionally shrinking the node if it becomes sufficiently sparse.

## Definition
```c
#define RT_REMOVE_CHILD_256 RT_MAKE_NAME(remove_child_256)
```

The actual function implementation:
```c
static inline void
RT_REMOVE_CHILD_256(RT_RADIX_TREE * tree, RT_PTR_ALLOC * parent_slot, RT_CHILD_PTR node, uint8 chunk)
```

## Detailed Description
This macro creates a template function name for removing a child pointer from a node256. The function marks the corresponding slot as free by clearing the appropriate bit in the isset bitmap, decrements the node's count, and checks if the node should be shrunk to a smaller node type (node48) based on a threshold calculation.

The function handles the special case where a full node256 has a count of zero due to overflow, ensuring that deletion occurs before checking shrink thresholds. The shrink threshold is calculated as the minimum of BITS_PER_BITMAPWORD and 3/4 of RT_FANOUT_48 to prevent unnecessary conversions between node types.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure
- `parent_slot`: Pointer to the parent's slot that references this node
- `node`: The node256 from which to remove the child
- `chunk`: The chunk value (key) identifying which child to remove

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for generating function names)
  - RT_BM_IDX (macro to get bitmap word index for chunk)
  - RT_BM_BIT (macro to get bit number within bitmap word)
  - RT_SHRINK_NODE_256 (conditionally called to shrink the node)
  - Min (minimum value macro)
- Called from (representative examples):
  - RT_NODE_DELETE (src/include/lib/radixtree.h:2597)

## Notes and Other Information
- This is part of PostgreSQL's template-based radix tree implementation
- Uses bitmap operations to efficiently mark slots as free: `isset[idx] &= ~((bitmapword) 1 << bitnum)`
- Handles overflow case where a full node256 has count=0 due to integer overflow
- Shrink threshold prevents ping-ponging between node types by using 3/4 occupancy rule
- The BITS_PER_BITMAPWORD limit ensures compatibility with RT_SHRINK_NODE_256's bitmap initialization
- Automatically triggers node shrinking when the node becomes sufficiently sparse