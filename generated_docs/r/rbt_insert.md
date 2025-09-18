# rbt_insert

## Location
src/backend/lib/rbtree.c: 453 - 520

## Overview
Inserts a new value into the Red-Black Tree, handling both new insertions and merging with existing nodes.

## Definition


## Detailed Description
This function provides the primary insertion interface for Red-Black Trees. It performs a standard binary search tree insertion followed by Red-Black Tree rebalancing. The function handles two scenarios:

1. **New insertion**: If the data represents a value not present in the tree, a new node is created, inserted at the appropriate location, and the tree is rebalanced.

2. **Existing value**: If a node with the same key already exists, the combiner function is called to merge the new data with the existing node.

The insertion process follows these steps:
1. Traverse the tree to find the insertion point using the comparator function
2. If a matching node is found, apply the combiner function and return the existing node
3. If no match is found, allocate a new red node using the tree's allocator function
4. Copy the data into the new node and insert it at the appropriate position
5. Call rbt_insert_fixup to restore Red-Black Tree properties

The data parameter represents the value to insert, but only the extra data beyond the RBTNode fields is of interest - the RBTNode fields are set up by this function.

## Parameters / Member Variables
- : Pointer to the Red-Black Tree structure
- : Pointer to the data to be inserted (RBTNode fields need not be initialized)
- : Pointer to boolean flag that will be set to true if a new node was created, false if existing node was updated

## Dependencies
- Functions called/Symbols referenced:
  - RBTree, RBTNode (tree and node structure types)
  - RBTNIL (sentinel value for null nodes)
  - RBTRED (red color constant)
  - color (node color field)
  - rbt_copy_data (copies data between nodes)
  - rbt_insert_fixup (rebalances tree after insertion)
- Called from (representative examples):
  - ginInsertBAEntry (in ginbulk.c:166)
  - rbt_populate (in test_rbtree.c:138, 150)

## Notes and Other Information
- This is a public API function for Red-Black Tree insertion
- Time complexity is O(log n) where n is the number of nodes in the tree
- The function is safe to call with existing keys due to the combiner mechanism
- New nodes are always initially colored red, with fixup handling any violations
- The data parameter is never modified - it's typically a local variable in the caller
- Returns a pointer to either the newly created node or the existing node that was updated
- Critical for maintaining Red-Black Tree properties while providing efficient insertion
- Uses the tree's configured comparator, combiner, and allocator functions for flexibility