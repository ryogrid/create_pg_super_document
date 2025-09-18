# plaintree

## Location
src/backend/utils/adt/tsquery_cleanup.c: 97 - 114

## Overview
The `plaintree` function serves as the main interface for converting a binary tree representation of a TSQuery back into a flat QueryItem array representation.

## Definition
```c
static QueryItem *plaintree(NODE *root, int *len)
```

## Detailed Description
The `plaintree` function is a wrapper function that orchestrates the conversion of a binary tree representation back into PostgreSQL's linear QueryItem array format. It initializes a PLAINTREE state structure and delegates the actual conversion work to the `plainnode` function.

The function first sets up the initial state with a starting buffer size of 16 QueryItem elements. It validates that the root node contains either a value (QI_VAL) or operator (QI_OPR) before proceeding with the conversion. If the root is valid, it allocates memory for the output array and calls `plainnode` to perform the recursive conversion. If the root is invalid or NULL, it returns NULL.

The function returns the resulting QueryItem array and sets the length through the output parameter, providing a complete interface for tree-to-array conversion.

## Parameters / Member Variables
- `root`: Pointer to the root NODE of the binary tree to be converted
- `len`: Output parameter that receives the length of the resulting QueryItem array

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](palloc.md) (memory allocation)
  - [plainnode](plainnode.md) (recursive tree traversal and conversion)
- Called from (representative examples):
  - [clean_NOT](../c/clean_NOT.md)
  - [cleanup_tsquery_stopwords](../c/cleanup_tsquery_stopwords.md)

## Notes and Other Information
- This function is part of PostgreSQL's text search query cleanup and optimization system
- Acts as the main entry point for tree-to-array conversion operations
- Uses an initial buffer size of 16 elements, which is dynamically expanded by `plainnode` if needed
- Performs input validation to ensure the root node is of an appropriate type
- The returned array must be freed by the caller when no longer needed
- Returns NULL for invalid or empty trees, making it safe to use in various contexts