# avlInsertNode

## Location
[src/bin/psql/crosstabview.c:529-559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/crosstabview.c#L529-L559)

## Overview
Recursively inserts a new pivot field into an AVL tree, creating a new node if the value doesn't exist, while maintaining tree balance and binary search tree properties.

## Definition
static void avlInsertNode(avl_tree *tree, avl_node **node, pivot_field field)

## Detailed Description
The avlInsertNode function implements the recursive insertion algorithm for AVL trees in PostgreSQL's crosstab view functionality. It traverses the tree to find the correct position for a new pivot field value, creating a new node when it reaches a leaf position (tree->end). If the value already exists in the tree, no insertion occurs, preventing duplicates. After each recursive insertion, the function calls avlAdjustBalance to maintain the AVL tree's balanced property. The function uses pivotFieldCompare to determine the insertion direction, following standard binary search tree ordering. New nodes are allocated with pg_malloc and initialized with height 1, with both children pointing to the tree's end marker.

## Parameters / Member Variables
- tree: Pointer to the AVL tree structure containing metadata and the end marker
- node: Double pointer to the current node in the recursion; may be modified to point to a new node
- field: The pivot_field value to be inserted into the tree

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc
  - [pivotFieldCompare](../p/pivotFieldCompare.md)
  - [avlInsertNode](avlInsertNode.md) (recursive call)
  - [avlAdjustBalance](avlAdjustBalance.md)
  - pivot_field
  - avl_tree
  - avl_node
- Called from (representative examples):
  - [avlInsertNode](avlInsertNode.md) (recursive)
  - [avlMergeValue](avlMergeValue.md)

## Notes and Other Information
This function is a core component of PostgreSQL's crosstab view implementation, which uses AVL trees to efficiently store and organize pivot field values for cross-tabulated query results. The recursive nature allows for natural tree traversal while maintaining the call stack for proper balance adjustment on the way back up. The function increments the tree's count only when actually inserting a new node, ensuring accurate tree size tracking. The use of tree->end as a sentinel value simplifies boundary checking and tree structure management.