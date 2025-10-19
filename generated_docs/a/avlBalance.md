# avlBalance

## Location
[src/bin/psql/crosstabview.c:495-505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/crosstabview.c#L495-L505)

## Overview
Calculates the balance factor of an AVL tree node by computing the height difference between its left and right subtrees.

## Definition
static int avlBalance(avl_node *n)

## Detailed Description
The avlBalance function computes the balance factor of a given AVL tree node, which is fundamental to maintaining the AVL tree's balanced property. The balance factor is calculated as the height of the left subtree minus the height of the right subtree. In a properly balanced AVL tree, this value must be -1, 0, or 1. If the balance factor exceeds this range, rotations are needed to restore balance. A positive balance factor indicates the left subtree is taller, while a negative value indicates the right subtree is taller.

## Parameters / Member Variables
- n: Pointer to the AVL tree node for which to calculate the balance factor

## Dependencies
- Functions called/Symbols referenced:
  - avl_node
- Called from (representative examples):
  - [avlAdjustBalance](avlAdjustBalance.md)

## Notes and Other Information
This function is a key component of AVL tree maintenance in PostgreSQL's crosstab view implementation. The balance factor is used by avlAdjustBalance to determine when and what type of rotations are needed to maintain tree balance. The function assumes that the node and its children are properly initialized with valid height values, which are maintained by avlUpdateHeight after tree modifications.

## Simplified Source

```c
static int avlBalance(avl_node *n) {
    // Balance factor = left_height - right_height
    // Positive: left subtree taller, Negative: right subtree taller
    return n->children[0]->height - n->children[1]->height;
}
```