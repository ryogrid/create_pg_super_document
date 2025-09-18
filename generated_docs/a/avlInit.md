# avlInit

## Location
[src/bin/psql/crosstabview.c:438-447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/crosstabview.c#L438-L447)

## Overview
Initializes an AVL binary tree structure used to efficiently collect distinct values that will form the horizontal and vertical headers in crosstab view.

## Definition
```c
static void avlInit(avl_tree *tree)
```

## Detailed Description
avlInit is a utility function that initializes an AVL (Adelson-Velsky and Landis) binary tree structure for use in the crosstab view functionality. This function sets up a minimalistic AVL tree implementation that is specifically designed for collecting distinct values without supporting removal or search operations.

The initialization creates a sentinel end node that serves as a boundary marker for the tree structure. The sentinel node points to itself in both child positions, providing a clean way to handle tree traversal edge cases. The tree starts empty with a count of zero and the root pointing to the sentinel node.

This AVL tree implementation is optimized for the specific use case of collecting unique header values during crosstab processing, where only insertion and in-order traversal operations are needed.

## Parameters / Member Variables
- `tree`: Pointer to the avl_tree structure to be initialized

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc0 (for allocating zeroed memory for the sentinel node)
  - avl_node (structure type for tree nodes)
  - avl_tree (structure type for the tree container)
- Called from (representative examples):
  - [PrintResultInCrosstab](../P/PrintResultInCrosstab.md) (src/bin/psql/crosstabview.c:119, 120)

## Notes and Other Information
- Creates a sentinel end node that acts as a boundary marker for tree operations
- The sentinel node is self-referential (both children point to itself) to simplify tree traversal logic
- Initial tree state has count=0 and root pointing to the sentinel node
- This is part of a minimalistic AVL tree implementation focused only on insertion and traversal
- The tree is used specifically for collecting distinct values during crosstab header generation
- Memory for the sentinel node is allocated using pg_malloc0 to ensure zero initialization