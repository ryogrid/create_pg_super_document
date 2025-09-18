# avlRotate

## Location
src/bin/psql/crosstabview.c: 481 - 494

## Overview
Performs a single AVL tree rotation (left or right) to maintain balance in the binary search tree used in PostgreSQL's crosstab view functionality.

## Definition
static avl_node *avlRotate(avl_node **current, int dir)

## Detailed Description
The avlRotate function implements a fundamental AVL tree operation that performs tree rotation to maintain the balanced property of an AVL tree. This is a non-recursive operation that rotates a subtree either left (dir=0) or right (dir=1). The function modifies the tree structure by rearranging parent-child relationships between nodes while preserving the binary search tree ordering property. After the rotation, it updates the height of the affected node using avlUpdateHeight to maintain accurate height information for future balance calculations.

## Parameters / Member Variables
- current: Double pointer to the root node of the subtree being rotated; modified to point to the new root after rotation
- dir: Direction of rotation (0 for left rotation, 1 for right rotation)

## Dependencies
- Functions called/Symbols referenced:
  - avlUpdateHeight
  - avl_node
- Called from (representative examples):
  - avlAdjustBalance

## Notes and Other Information
This function is part of PostgreSQL's crosstab view implementation in psql, which uses an AVL tree to efficiently organize and display cross-tabulated query results. The rotation operation is essential for maintaining O(log n) performance characteristics of the AVL tree by ensuring the tree remains balanced. The function operates by swapping the roles of a parent and child node while correctly updating all child pointers to maintain tree integrity.