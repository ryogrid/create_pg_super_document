# rbt_rotate_right

## Location
[src/backend/lib/rbtree.c:300-343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L300-L343)

## Overview
Performs a right rotation operation on a Red-Black Tree node, restructuring the tree to maintain balance while preserving the binary search tree property.

## Definition

```c
static void
rbt_rotate_right(RBTree *rbt, RBTNode *x)
```
## Detailed Description
This function performs a right rotation, which is the mirror operation of left rotation. In a right rotation, node x's left child (y) takes x's place in the tree, and x becomes the right child of y.

The rotation process involves three main steps:
1. Establish new parent-child relationships between x and y's right subtree  
2. Update y's parent link to point to x's former parent
3. Complete the rotation by making x the right child of y

This operation, like left rotation, preserves the binary search tree invariant while helping to maintain Red-Black Tree balance properties.

## Parameters / Member Variables
- `*rbt`: Pointer to the Red-Black Tree structure
- `*x`: The node around which to perform the right rotation
## Dependencies
- Functions called/Symbols referenced:
  - [RBTree](../R/RBTree.md) (tree structure type)
  - [RBTNode](../R/RBTNode.md) (node structure type)  
  - RBTNIL (sentinel value for null nodes)
- Called from (representative examples):
  - [rbt_insert_fixup](rbt_insert_fixup.md) (in rbtree.c:395, 418)
  - [rbt_delete_fixup](rbt_delete_fixup.md) (in rbtree.c:563, 583, 607)

## Notes and Other Information
- This is a static (internal) function not exposed in the public API
- Time complexity is O(1) - constant time operation
- The function assumes that x->left is not RBTNIL (has a valid left child)
- Right rotations are typically used during insertion and deletion fixup operations to restore Red-Black Tree properties
- The operation is the inverse of rbt_rotate_left
- Critical for maintaining logarithmic height bounds in Red-Black Trees
- Handles edge cases where x is the root node by updating rbt->root appropriately
- Works in conjunction with left rotations to rebalance the tree during modifications

## Simplified Source

```c
static void
rbt_rotate_right(RBTree *rbt, RBTNode *x)
{
    RBTNode *y = x->left;  // y will take x's place

    // Step 1: Move y's right subtree to x's left
    x->left = y->right;
    if (y->right != RBTNIL)
        y->right->parent = x;

    // Step 2: Update y's parent connection
    if (y != RBTNIL)
        y->parent = x->parent;

    if (x->parent)
    {
        if (x == x->parent->right)
            x->parent->right = y;
        else
            x->parent->left = y;
    }
    else
    {
        rbt->root = y;  // y becomes new root
    }

    // Step 3: Make x the right child of y
    y->right = x;
    if (x != RBTNIL)
        x->parent = y;
}
```