# rbt_delete_node

## Location
src/backend/lib/rbtree.c: 619 - 694

## Overview
Deletes a specified node from a Red-Black tree while maintaining tree structure and preparing for balance restoration if necessary.

## Definition


## Detailed Description
This function implements the core node deletion logic for Red-Black trees. It handles the complex process of removing a node while preserving the binary search tree property and preparing for Red-Black tree balance maintenance. The algorithm follows the standard approach of finding the node to actually remove (which may be the target node itself or its tree successor), performing the structural removal, and then calling the fixup routine if a black node was removed.

The function handles three main cases: deleting a node with no children, one child, or two children. For nodes with two children, it uses the tree successor replacement strategy. After the structural deletion, if a black node was removed, it calls rbt_delete_fixup to restore Red-Black tree properties.

## Parameters / Member Variables
- : Pointer to the Red-Black tree structure from which the node will be deleted
- : Pointer to the node to be deleted from the tree

## Dependencies
- Functions called/Symbols referenced:
  - [rbt_copy_data](rbt_copy_data.md)
  - [rbt_delete_fixup](rbt_delete_fixup.md)
  - RBTNIL (sentinel node constant)
  - RBTBLACK (color constant)
  - [RBTree](../R/RBTree.md) (tree structure type)
  - [RBTNode](../R/RBTNode.md) (node structure type)
- Called from (representative examples):
  - [rbt_delete](rbt_delete.md)

## Notes and Other Information
- The function includes paranoia checking to ensure it's only called on valid nodes
- Uses the tree successor strategy for nodes with two children to maintain binary search tree properties
- Only calls the expensive fixup routine when a black node is removed, as removing red nodes doesn't violate Red-Black properties
- Properly handles memory management by calling the tree's freefunc if available
- This is an internal static function, not exposed in the public API
- The algorithm ensures that the binary search tree property is maintained throughout the deletion process