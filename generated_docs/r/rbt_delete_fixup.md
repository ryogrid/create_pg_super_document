# rbt_delete_fixup

## Location
[src/backend/lib/rbtree.c:521-618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L521-L618)

## Overview
Maintains Red-Black tree balance properties after deleting a black node by performing rotations and recoloring operations to restore the tree's balance.

## Definition

```c
static void
rbt_delete_fixup(RBTree *rbt, RBTNode *x)
```
## Detailed Description
This function is a critical component of the Red-Black tree deletion algorithm that restores the Red-Black tree properties after a black node has been removed. The deletion of a black node can violate the Red-Black tree's fundamental property that all paths from any node to its descendant leaf nodes must contain the same number of black nodes (black-height property).

The function operates by moving the "extra blackness" problem up the tree through a series of cases, each handled by specific combinations of rotations and recolorings. The algorithm considers symmetric left and right cases, systematically addressing different scenarios based on the color and position of the sibling node and its children. The process continues until either the problem is resolved or the extra blackness reaches the root, where it can be safely absorbed.

## Parameters / Member Variables
- `*rbt`: Pointer to the Red-Black tree structure being modified
- `*x`: Pointer to the black node that needs fixup (initially the former child of the deleted node)
## Dependencies
- Functions called/Symbols referenced:
  - [rbt_rotate_left](rbt_rotate_left.md)
  - [rbt_rotate_right](rbt_rotate_right.md)
  - RBTBLACK (color constant)
  - RBTRED (color constant)
  - [RBTree](../R/RBTree.md) (tree structure type)
  - [RBTNode](../R/RBTNode.md) (node structure type)
- Called from (representative examples):
  - [rbt_delete_node](rbt_delete_node.md)

## Notes and Other Information
- The function assumes that  is always a black node upon entry
- The algorithm handles symmetric left and right cases to maintain code clarity and correctness
- Each iteration moves the problem node higher up in the tree until the Red-Black properties are restored
- The loop terminates when either the problem reaches the root or is resolved through recoloring and rotations
- This is an internal static function, not exposed in the public API
- The implementation follows the classic Red-Black tree deletion fixup algorithm from computer science literature

## Simplified Source

```c
static void
rbt_delete_fixup(RBTree *rbt, RBTNode *x)
{
    // Fix red-black violations after deleting a black node
    // x has "extra blackness" that needs to be resolved
    while (x != rbt->root && x->color == RBTBLACK)
    {
        if (x == x->parent->left)
        {
            // x is left child - sibling is right child
            RBTNode *sibling = x->parent->right;

            // Case 1: Red sibling - convert to black sibling case
            if (sibling->color == RBTRED)
            {
                sibling->color = RBTBLACK;
                x->parent->color = RBTRED;
                rbt_rotate_left(rbt, x->parent);
                sibling = x->parent->right;
            }

            // Case 2: Black sibling with black children - move problem up
            if (sibling->left->color == RBTBLACK &&
                sibling->right->color == RBTBLACK)
            {
                sibling->color = RBTRED;
                x = x->parent;  // Move problem up the tree
            }
            else
            {
                // Case 3: Black sibling with red child - fix via rotations
                if (sibling->right->color == RBTBLACK)
                {
                    // Convert to case 4
                    sibling->left->color = RBTBLACK;
                    sibling->color = RBTRED;
                    rbt_rotate_right(rbt, sibling);
                    sibling = x->parent->right;
                }

                // Case 4: Final fixup with rotation
                sibling->color = x->parent->color;
                x->parent->color = RBTBLACK;
                sibling->right->color = RBTBLACK;
                rbt_rotate_left(rbt, x->parent);
                x = rbt->root;  // Problem solved, exit loop
            }
        }
        else
        {
            // Mirror case: x is right child
            RBTNode *sibling = x->parent->left;

            if (sibling->color == RBTRED)
            {
                sibling->color = RBTBLACK;
                x->parent->color = RBTRED;
                rbt_rotate_right(rbt, x->parent);
                sibling = x->parent->left;
            }

            if (sibling->right->color == RBTBLACK &&
                sibling->left->color == RBTBLACK)
            {
                sibling->color = RBTRED;
                x = x->parent;
            }
            else
            {
                if (sibling->left->color == RBTBLACK)
                {
                    sibling->right->color = RBTBLACK;
                    sibling->color = RBTRED;
                    rbt_rotate_left(rbt, sibling);
                    sibling = x->parent->left;
                }
                sibling->color = x->parent->color;
                x->parent->color = RBTBLACK;
                sibling->left->color = RBTBLACK;
                rbt_rotate_right(rbt, x->parent);
                x = rbt->root;
            }
        }
    }

    // Ensure final node is black
    x->color = RBTBLACK;
}
```