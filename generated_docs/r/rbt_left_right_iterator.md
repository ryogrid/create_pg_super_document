# rbt_left_right_iterator

## Location
src/backend/lib/rbtree.c: 705 - 746

## Overview
Implements in-order (left-to-right) traversal logic for Red-Black tree iteration, returning nodes in ascending key order.

## Definition


## Detailed Description
This function provides the core logic for in-order traversal of a Red-Black tree, which visits nodes in ascending order of their keys. The algorithm implements the standard in-order traversal pattern: visit left subtree, process current node, then visit right subtree. The function maintains state through the iterator structure, allowing for step-by-step traversal without recursion.

The algorithm handles three main cases: initial traversal (finding the leftmost node), moving to the next node when the current node has a right subtree (finding the leftmost node in that subtree), and backtracking up the tree when no right subtree exists. The traversal continues until all nodes have been visited, at which point the iterator is marked as complete.

## Parameters / Member Variables
- : Pointer to the RBTreeIterator structure that maintains traversal state and tree reference

## Dependencies
- Functions called/Symbols referenced:
  - RBTNIL (sentinel node constant)
  - RBTreeIterator (iterator structure type)
  - RBTNode (node structure type)
- Called from (representative examples):
  - rbt_begin_iterate

## Notes and Other Information
- This is an internal static function used by the public iteration API
- Implements non-recursive in-order traversal using the iterator's state
- Returns nodes in ascending order based on the tree's comparison function
- The algorithm efficiently handles the three cases of in-order traversal without stack overhead
- Sets the is_over flag when traversal is complete
- Maintains the last_visited pointer to track current position in the tree
- The function assumes proper iterator initialization before first call
- Used as part of PostgreSQL's Red-Black tree iteration framework for ordered data access