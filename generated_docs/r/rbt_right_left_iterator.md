# rbt_right_left_iterator

## Location
[src/backend/lib/rbtree.c:747-801](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L747-L801)

## Overview
Implements reverse in-order (right-to-left) traversal logic for Red-Black tree iteration, returning nodes in descending key order.

## Definition


## Detailed Description
This function provides the core logic for reverse in-order traversal of a Red-Black tree, which visits nodes in descending order of their keys. The algorithm implements the reverse of standard in-order traversal: visit right subtree, process current node, then visit left subtree. Like its left-right counterpart, it maintains state through the iterator structure for step-by-step traversal without recursion.

The algorithm mirrors the left-right iterator but with reversed logic: it handles initial traversal by finding the rightmost node, moves to the next node when the current node has a left subtree (finding the rightmost node in that subtree), and backtracks up the tree when no left subtree exists. This provides an efficient way to iterate through tree elements in reverse sorted order.

## Parameters / Member Variables
- : Pointer to the RBTreeIterator structure that maintains traversal state and tree reference

## Dependencies
- Functions called/Symbols referenced:
  - RBTNIL (sentinel node constant)
  - [RBTreeIterator](../R/RBTreeIterator.md) (iterator structure type)
  - [RBTNode](../R/RBTNode.md) (node structure type)
- Called from (representative examples):
  - [rbt_begin_iterate](rbt_begin_iterate.md)

## Notes and Other Information
- This is an internal static function used by the public iteration API
- Implements non-recursive reverse in-order traversal using the iterator's state
- Returns nodes in descending order based on the tree's comparison function
- Provides the symmetric counterpart to rbt_left_right_iterator for reverse iteration
- Sets the is_over flag when traversal is complete
- Maintains the last_visited pointer to track current position in the tree
- The function assumes proper iterator initialization before first call
- Used as part of PostgreSQL's Red-Black tree iteration framework for reverse-ordered data access
- Particularly useful when applications need to process data in descending order without additional sorting