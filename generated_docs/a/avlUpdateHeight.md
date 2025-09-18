# avlUpdateHeight

## Location
src/bin/psql/crosstabview.c: 472 - 480

## Overview
Updates the height value of an AVL tree node based on the heights of its children, maintaining the height property required for AVL tree balance operations.

## Definition
```c
static void avlUpdateHeight(avl_node *n)
```

## Detailed Description
avlUpdateHeight is a utility function that recalculates and updates the height value of a specific AVL tree node. The height of a node in an AVL tree is defined as 1 plus the maximum height of its children. This height information is crucial for maintaining the AVL tree's balance property and performing rotations when needed.

The function implements the standard AVL height calculation: it examines both children of the node, determines which child has the greater height, and sets the node's height to that maximum value plus one. This ensures that the height property is correctly maintained throughout tree operations.

This function is typically called after tree modifications (insertions, rotations) to ensure that height values remain accurate, which is essential for detecting imbalances and triggering rebalancing operations in the AVL tree.

## Parameters / Member Variables
- `n`: Pointer to the avl_node whose height needs to be updated

## Dependencies
- Functions called/Symbols referenced:
  - avl_node (structure type for tree nodes)
- Called from (representative examples):
  - avlRotate (src/bin/psql/crosstabview.c:488)
  - avlAdjustBalance (src/bin/psql/crosstabview.c:520)

## Notes and Other Information
- Implements the standard AVL tree height calculation: max(left_height, right_height) + 1
- Essential for maintaining AVL tree balance properties
- Called during tree modification operations to keep height values accurate
- Height information is used to detect imbalances and trigger rotations
- Part of the minimalistic AVL tree implementation used for crosstab distinct value collection
- Assumes that child nodes already have correct height values
- Simple but critical operation for AVL tree self-balancing functionality