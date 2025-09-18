# RT_ADD_CHILD_256

## Location
src/include/lib/radixtree.h: 1272 - 1287

## Overview
A macro that resolves to a static inline function for adding a child node to a 256-way radix tree node by marking the appropriate slot as used in the bitmap array.

## Definition
```c
#define RT_ADD_CHILD_256 RT_MAKE_NAME(add_child_256)

static inline RT_PTR_ALLOC *
RT_ADD_CHILD_256(RT_RADIX_TREE * tree, RT_CHILD_PTR node, uint8 chunk)
```

## Detailed Description
This function adds a child to a node256 (256-way node) in the radix tree by marking the slot corresponding to the given chunk as used in the bitmap array. Node256 uses a bitmap to track which of the 256 possible slots are occupied, allowing direct indexing by chunk value. The function updates the bitmap, increments the node's count, and returns a pointer to the child slot where the new child can be stored.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure
- `node`: The node256 to add a child to
- `chunk`: The 8-bit key fragment that determines which slot to use (0-255)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for name generation)
  - RT_BM_IDX (gets bitmap array index for chunk)
  - RT_BM_BIT (gets bit position within bitmap word)
  - RT_VERIFY_NODE (node verification macro)
  - RT_NODE_256_GET_CHILD (gets child pointer for chunk)
- Called from (representative examples):
  - RT_GROW_NODE_48 (at src/include/lib/radixtree.h:1331)
  - RT_NODE_INSERT (at src/include/lib/radixtree.h:1570)

## Notes and Other Information
Node256 is the largest node type in the radix tree, supporting up to 256 children with direct indexing by chunk value. The bitmap array `isset` tracks which slots are occupied, allowing efficient space usage even when the node is sparsely populated. This is the most straightforward add operation since no array shifting or searching is required - the chunk value directly determines the storage location.