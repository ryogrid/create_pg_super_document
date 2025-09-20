# _avl_tree

## Location
[src/bin/psql/crosstabview.c:73-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/crosstabview.c#L73-L77)

## Overview
The _avl_tree structure serves as the control structure for an AVL (Adelson-Velsky and Landis) balanced binary search tree used to manage pivot fields in psql's crosstabview functionality.

## Definition

```c
typedef struct _avl_tree
{
	int			count;			/* Total number of nodes */
	avl_node   *root;			/* root of the tree */
	avl_node   *end;			/* Immutable dereferenceable empty tree */
} avl_tree;
```
## Detailed Description
The _avl_tree structure provides the management interface for an AVL balanced binary search tree implementation specifically designed for organizing pivot fields in PostgreSQL's psql crosstabview feature. AVL trees maintain optimal search performance by automatically balancing themselves during insertions and deletions, ensuring O(log n) time complexity for all major operations.

This structure contains the essential metadata needed to manage the tree: a count of total nodes for efficient size queries, a pointer to the root node for tree traversals, and a special 'end' node that serves as a sentinel value. The end node is particularly important as it's used instead of NULL pointers for non-existent children, which simplifies tree algorithms by eliminating the need for NULL checks during traversals and rotations.

## Parameters / Member Variables
- : Total number of nodes currently stored in the tree, used for efficient size operations and tree management
- : Pointer to the root node of the AVL tree; represents the entry point for all tree operations like search, insertion, and traversal
- : Pointer to an immutable, dereferenceable empty tree node used as a sentinel value; child pointers reference this instead of NULL when no child exists, simplifying tree algorithms

## Dependencies
- Functions called/Symbols referenced:
  - avl_node (for root and end node pointers)
- Called from (representative examples):
  - avl_tree (function declarations and usage)
  - [PrintResultInCrosstab](../P/PrintResultInCrosstab.md) (for crosstab processing)
  - [avlInit](avlInit.md) (for tree initialization)
  - [avlFree](avlFree.md) (for memory cleanup)
  - [avlAdjustBalance](avlAdjustBalance.md) (for balancing operations)
  - [avlInsertNode](avlInsertNode.md) (for node insertion)
  - [avlMergeValue](avlMergeValue.md) (for value merging)
  - [avlCollectFields](avlCollectFields.md) (for field collection and traversal)

## Notes and Other Information
- Part of psql's crosstabview AVL tree implementation located in src/bin/psql/crosstabview.c:73-77
- The sentinel end node design is a key optimization that eliminates NULL pointer checks in tree algorithms
- Used to maintain sorted collections of pivot fields with guaranteed logarithmic performance
- The typedef creates the alias 'avl_tree' for easier usage throughout the crosstabview module
- Integrates with the broader crosstabview system for transforming SQL result sets into pivot table formats
- The tree structure supports efficient insertion, search, and in-order traversal required for crosstab header generation