# avlCollectFields

## Location
[src/bin/psql/crosstabview.c:577-587](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/crosstabview.c#L577-L587)

## Overview
Recursively extracts node values from an AVL tree into a fields array in sorted order using an in-order tree traversal.

## Definition

```c
static int
avlCollectFields(avl_tree *tree, avl_node *node, pivot_field *fields, int idx)
```
## Detailed Description
This function performs an in-order traversal of an AVL tree to collect all pivot field values in sorted order. It recursively visits the left subtree, processes the current node by copying its field value to the fields array, then visits the right subtree. The function maintains an index parameter that tracks the current position in the output array and returns the next available index for subsequent writes.

The function is specifically designed for the PostgreSQL psql crosstab view feature, where it extracts sorted field values from an AVL tree that maintains unique pivot values for generating crosstab reports.

## Parameters / Member Variables
- `tree`: Pointer to the AVL tree structure containing the nodes to traverse
- `node`: Current node being processed in the traversal (can be tree->end for empty subtrees)
- `fields`: Pre-allocated array to store the collected pivot_field values in sorted order
- `idx`: Current index position in the fields array where the next field should be written

## Dependencies
- Functions called/Symbols referenced:
  - [avlCollectFields](avlCollectFields.md) (recursive calls for left and right subtrees)
- Called from (representative examples):
  - [PrintResultInCrosstab](../P/PrintResultInCrosstab.md) (main crosstab processing function)

## Notes and Other Information
- The fields array must be pre-allocated to hold exactly tree->count entries before calling this function
- The function uses the AVL tree's natural ordering to produce sorted output without additional sorting
- Returns the next available index in the fields array, enabling chaining of multiple tree collections
- Uses tree->end as a sentinel value to detect empty subtrees and terminate recursion
- This is a key component of the PostgreSQL psql \crosstabview feature for generating pivot table outputs