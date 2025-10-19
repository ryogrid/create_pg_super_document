# rbt_left_right_iterator

## Location
[src/backend/lib/rbtree.c:705-746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L705-L746)

## Overview
Implements in-order (left-to-right) traversal logic for Red-Black tree iteration, returning nodes in ascending key order.

## Definition

```c
static RBTNode *
rbt_left_right_iterator(RBTreeIterator *iter)
```
## Detailed Description
This function provides the core logic for in-order traversal of a Red-Black tree, which visits nodes in ascending order of their keys. The algorithm implements the standard in-order traversal pattern: visit left subtree, process current node, then visit right subtree. The function maintains state through the iterator structure, allowing for step-by-step traversal without recursion.

The algorithm handles three main cases: initial traversal (finding the leftmost node), moving to the next node when the current node has a right subtree (finding the leftmost node in that subtree), and backtracking up the tree when no right subtree exists. The traversal continues until all nodes have been visited, at which point the iterator is marked as complete.

## Parameters / Member Variables
- `*iter`: Pointer to the RBTreeIterator structure that maintains traversal state and tree reference
## Dependencies
- Functions called/Symbols referenced:
  - RBTNIL (sentinel node constant)
  - [RBTreeIterator](../R/RBTreeIterator.md) (iterator structure type)
  - [RBTNode](../R/RBTNode.md) (node structure type)
- Called from (representative examples):
  - [rbt_begin_iterate](rbt_begin_iterate.md)

## Notes and Other Information
- This is an internal static function used by the public iteration API
- Implements non-recursive in-order traversal using the iterator's state
- Returns nodes in ascending order based on the tree's comparison function
- The algorithm efficiently handles the three cases of in-order traversal without stack overhead
- Sets the is_over flag when traversal is complete
- Maintains the last_visited pointer to track current position in the tree
- The function assumes proper iterator initialization before first call
- Used as part of PostgreSQL's Red-Black tree iteration framework for ordered data access

## Simplified Source

```c
static RBTNode *
rbt_left_right_iterator(RBTreeIterator *iter)
{
    // First call: find leftmost node in tree
    if (iter->last_visited == NULL) {
        iter->last_visited = iter->rbt->root;
        while (iter->last_visited->left != RBTNIL)
            iter->last_visited = iter->last_visited->left;
        return iter->last_visited;
    }

    // If current node has right subtree, find leftmost in that subtree
    if (iter->last_visited->right != RBTNIL) {
        iter->last_visited = iter->last_visited->right;
        while (iter->last_visited->left != RBTNIL)
            iter->last_visited = iter->last_visited->left;
        return iter->last_visited;
    }

    // Move up tree until we find a node we came to from the left
    for (;;) {
        RBTNode *came_from = iter->last_visited;
        iter->last_visited = iter->last_visited->parent;

        if (iter->last_visited == NULL) {
            iter->is_over = true;
            break;
        }

        // If we came from left subtree, this is next node
        if (iter->last_visited->left == came_from)
            break;
    }

    return iter->last_visited;
}
```