# avlMergeValue

## Location
src/bin/psql/crosstabview.c: 560 - 576

## Overview
High-level wrapper function that creates a pivot field from provided values and inserts it into an AVL tree if it doesn't already exist.

## Definition
static void avlMergeValue(avl_tree *tree, char *name, char *sort_value)

## Detailed Description
The avlMergeValue function provides a convenient interface for inserting pivot field values into an AVL tree used in PostgreSQL's crosstab view functionality. It constructs a pivot_field structure from the provided name and sort_value parameters, assigns it a rank based on the current tree count, and then calls avlInsertNode to perform the actual insertion. The function serves as a higher-level abstraction that simplifies the process of adding new values to the tree by handling the pivot_field construction and ensuring proper rank assignment. If the value already exists in the tree, no insertion occurs due to the duplicate prevention logic in avlInsertNode.

## Parameters / Member Variables
- tree: Pointer to the AVL tree where the value should be inserted
- name: String representation of the pivot field name
- sort_value: String value used for sorting and comparison purposes

## Dependencies
- Functions called/Symbols referenced:
  - avlInsertNode
  - pivot_field
  - avl_tree
- Called from (representative examples):
  - PrintResultInCrosstab

## Notes and Other Information
This function is part of PostgreSQL's crosstab view implementation in psql, which allows users to display query results in a cross-tabulated format. The rank assignment using tree->count ensures that fields are numbered in the order they are first encountered. The function is typically called during the processing of query results to build up the set of unique pivot values that will become columns in the cross-tabulated output. The separation of concerns between avlMergeValue (field construction) and avlInsertNode (tree insertion) makes the code more maintainable and provides clear abstraction layers.