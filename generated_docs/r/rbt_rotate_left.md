# rbt_rotate_left

## Location
src/backend/lib/rbtree.c: 263 - 299

## Overview
Performs a left rotation operation on a Red-Black Tree node, restructuring the tree to maintain balance while preserving the binary search tree property.

## Definition


## Detailed Description
This function performs a fundamental tree rotation operation that is essential for maintaining Red-Black Tree balance properties. In a left rotation, node x's right child (y) takes x's place in the tree, and x becomes the left child of y.

The rotation process involves three main steps:
1. Establish new parent-child relationships between x and y's left subtree
2. Update y's parent link to point to x's former parent
3. Complete the rotation by making x the left child of y

This operation preserves the binary search tree invariant: all nodes in the left subtree have values less than the parent, and all nodes in the right subtree have values greater than the parent.

## Parameters / Member Variables
- : Pointer to the Red-Black Tree structure
- : The node around which to perform the left rotation

## Dependencies
- Functions called/Symbols referenced:
  - RBTree (tree structure type)
  - RBTNode (node structure type)
  - RBTNIL (sentinel value for null nodes)
- Called from (representative examples):
  - rbt_insert_fixup (in rbtree.c:388, 423)
  - rbt_delete_fixup (in rbtree.c:546, 570, 600)

## Notes and Other Information
- This is a static (internal) function not exposed in the public API
- Time complexity is O(1) - constant time operation
- The function assumes that x->right is not RBTNIL (has a valid right child)
- Left rotations are typically used during insertion and deletion fixup operations to restore Red-Black Tree properties
- The operation is reversible via rbt_rotate_right
- Critical for maintaining logarithmic height bounds in Red-Black Trees
- Handles edge cases where x is the root node by updating rbt->root appropriately