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
- : Pointer to the Red-Black tree structure being modified
- : Pointer to the black node that needs fixup (initially the former child of the deleted node)

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