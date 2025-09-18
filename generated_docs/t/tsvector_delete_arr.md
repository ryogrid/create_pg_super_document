# tsvector_delete_arr

## Location
src/backend/utils/adt/tsvector_op.c: 578 - 631

## Overview
Deletes multiple lexemes from a TSVector by accepting an array of lexemes to remove, implementing the user-level ts_delete(tsvector, text[]) function.

## Definition
```c
Datum tsvector_delete_arr(PG_FUNCTION_ARGS)
```

## Detailed Description
This function removes multiple lexemes from a TSVector by processing an array of text elements. It deconstructs the input array into individual lexeme strings and performs binary search for each lexeme in the TSVector. The function is optimized for the typical use case where the array of lexemes to delete is relatively small compared to the TSVector size.

For each non-null lexeme in the array, the function performs a binary search to find its position in the TSVector. Found positions are collected in a skip_indices array, which is then passed to tsvector_delete_by_indices for efficient batch deletion. The function handles null array elements gracefully by ignoring them.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Input TSVector from which to delete lexemes
- `PG_FUNCTION_ARGS[1]`: Array of text elements containing lexemes to delete

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR - Extract TSVector argument
  - PG_GETARG_ARRAYTYPE_P - Extract array argument
  - deconstruct_array_builtin - Decompose array into elements
  - VARDATA - Get pointer to variable-length data
  - VARSIZE - Get size of variable-length data
  - tsvector_bsearch - Binary search for lexeme in TSVector
  - tsvector_delete_by_indices - Helper to delete lexemes by index array
  - palloc0 - Allocate zero-initialized memory
  - pfree - Free allocated memory
  - PG_FREE_IF_COPY - Free copied arguments if needed
  - PG_RETURN_POINTER - Return result pointer
- Called from (representative examples):
  - No direct references found (likely called through SQL function dispatch)

## Notes and Other Information
- Optimized for small arrays of lexemes to delete relative to TSVector size
- Handles null array elements by skipping them during processing
- Uses binary search for efficient lexeme lookup in the sorted TSVector structure
- Collects all matching indices before performing batch deletion for efficiency
- Proper memory management with cleanup of temporary arrays and copied arguments
- Part of PostgreSQL's full-text search functionality for batch TSVector manipulation
- More efficient than calling tsvector_delete_str multiple times for multiple deletions