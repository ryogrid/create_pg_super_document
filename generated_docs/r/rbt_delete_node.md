# rbt_delete_node

## Location
[src/backend/lib/rbtree.c:619-694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L619-L694)

## Overview
Deletes a specified node from a Red-Black tree while maintaining tree structure and preparing for balance restoration if necessary.

## Definition

```c
static void
rbt_delete_node(RBTree *rbt, RBTNode *z)
```
## Detailed Description
This function implements the core node deletion logic for Red-Black trees. It handles the complex process of removing a node while preserving the binary search tree property and preparing for Red-Black tree balance maintenance. The algorithm follows the standard approach of finding the node to actually remove (which may be the target node itself or its tree successor), performing the structural removal, and then calling the fixup routine if a black node was removed.

The function handles three main cases: deleting a node with no children, one child, or two children. For nodes with two children, it uses the tree successor replacement strategy. After the structural deletion, if a black node was removed, it calls rbt_delete_fixup to restore Red-Black tree properties.

## Parameters / Member Variables
- `*rbt`: Pointer to the Red-Black tree structure from which the node will be deleted
- `*z`: Pointer to the node to be deleted from the tree
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

## Simplified Source

```c
static void
rbt_delete_node(RBTree *rbt, RBTNode *z)
{
    RBTNode *x, *y;

    // Paranoia check - only delete valid nodes
    if (!z || z == RBTNIL)
        return;

    // Determine which node to actually remove
    if (z->left == RBTNIL || z->right == RBTNIL)
    {
        // z has 0 or 1 child - remove z directly
        y = z;
    }
    else
    {
        // z has 2 children - find successor to replace it
        y = z->right;
        while (y->left != RBTNIL)
            y = y->left;  // Find leftmost node in right subtree
    }

    // x is y's only child (or RBTNIL if no child)
    if (y->left != RBTNIL)
        x = y->left;
    else
        x = y->right;

    // Remove y from tree by linking its child to its parent
    x->parent = y->parent;
    if (y->parent)
    {
        if (y == y->parent->left)
            y->parent->left = x;
        else
            y->parent->right = x;
    }
    else
    {
        rbt->root = x;  // y was root, x becomes new root
    }

    // If we removed successor instead of z, copy successor's data to z
    if (y != z)
        rbt_copy_data(rbt, z, y);

    // If we removed a black node, fix red-black violations
    if (y->color == RBTBLACK)
        rbt_delete_fixup(rbt, x);

    // Clean up the removed node
    if (rbt->freefunc)
        rbt->freefunc(y, rbt->arg);
}
```