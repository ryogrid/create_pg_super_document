# RT_REMOVE_CHILD_48

## Location
src/include/lib/radixtree.h: 2440 - 2472

## Overview
RT_REMOVE_CHILD_48 is a macro that expands to a function responsible for removing a child entry from a node48 in PostgreSQL's radix tree implementation.

## Definition
```c
#define RT_REMOVE_CHILD_48 RT_MAKE_NAME(remove_child_48)

static void
RT_REMOVE_CHILD_48(RT_RADIX_TREE *tree, RT_PTR_ALLOC *parent_slot, RT_CHILD_PTR node, uint8 chunk)
```

## Detailed Description
This function removes a child entry from a node48 structure in the radix tree. A node48 is an intermediate node type that can hold up to 48 children and uses an indirection array (slot_idxs) to map chunk values to actual slot positions. The function performs the removal by:

1. Locating the slot position for the given chunk using the slot_idxs array
2. Clearing the corresponding bit in the isset bitmap to mark the slot as unused
3. Setting the slot_idxs entry to RT_INVALID_SLOT_IDX to invalidate the mapping
4. Decrementing the node's count
5. Potentially shrinking the node to a smaller node type if the count falls below the shrink threshold

The function includes an optimization where if the node becomes too sparse (count <= shrink_threshold), it automatically triggers node shrinking to maintain performance.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure
- `parent_slot`: Pointer to the parent's slot that contains this node (for potential node replacement)
- `node`: The node48 from which to remove the child
- `chunk`: The 8-bit chunk value identifying which child to remove

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - RT_BM_IDX
  - RT_BM_BIT  
  - RT_SHRINK_NODE_48
- Called from (representative examples):
  - RT_NODE_DELETE

## Notes and Other Information
- This is part of PostgreSQL's generic radix tree implementation that uses C macros for type polymorphism
- The node48 type uses a bitmap (isset) to efficiently track which slots are in use
- The shrink_threshold is set to 3/4 of RT_FANOUT_16_LO to prevent excessive oscillation between node types
- The function is marked as static and uses the pg_noinline attribute for the actual implementation
- Located in src/include/lib/radixtree.h at lines 2440-2465