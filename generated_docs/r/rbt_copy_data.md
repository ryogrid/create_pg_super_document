# rbt_copy_data

## Location
src/backend/lib/rbtree.c: 127 - 144

## Overview
Copies additional data fields from one Red-Black Tree node to another, excluding the base RBTNode structure fields.

## Definition


## Detailed Description
This is a static inline utility function that performs a memory copy of the user-defined data portion of an RBTNode. It copies only the additional fields beyond the base RBTNode structure, using the tree's node_size to determine how much data to copy. The function uses pointer arithmetic to skip past the base RBTNode structure and copy only the application-specific data that follows it.

## Parameters / Member Variables
- : Pointer to the RBTree structure (used to get node_size)
- : Destination RBTNode where data will be copied to
- : Source RBTNode from which data will be copied (const to prevent modification)

## Dependencies
- Functions called/Symbols referenced:
  - RBTree (structure type)
  - RBTNode (structure type)
  - memcpy (memory copy function)
- Called from (representative examples):
  - rbt_insert (during node insertion operations)
  - rbt_delete_node (during node deletion operations)

## Notes and Other Information
- This is a static inline function, meaning it's internal to the rbtree.c module and inlined for performance
- Uses pointer arithmetic (dest + 1, src + 1) to skip the base RBTNode structure
- The copy size is calculated as (node_size - sizeof(RBTNode)) to copy only the additional data
- Essential for maintaining data integrity when nodes are moved or copied during tree rebalancing operations
- Does not validate input parameters, assuming they are valid RBTNode pointers from the same tree type