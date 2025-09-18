# array_smaller

## Location
src/backend/utils/adt/arrayfuncs.c: 5884 - 5892

## Overview
array_smaller returns the lexicographically smaller of two arrays by comparing them using array comparison semantics.

## Definition
```c
Datum
array_smaller(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL LEAST function for arrays by performing lexicographic comparison using array_cmp. It returns the first array if it is lexicographically less than the second array, otherwise it returns the second array. The comparison follows PostgreSQL's standard array comparison rules, comparing elements pairwise from left to right until a difference is found.

## Parameters / Member Variables
- Uses PostgreSQL function call convention via PG_FUNCTION_ARGS:
  - First argument: First array for comparison
  - Second argument: Second array for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [array_cmp](array_cmp.md) (performs the actual lexicographic comparison between arrays)
  - PG_RETURN_DATUM (macro for returning Datum values)
  - PG_GETARG_DATUM (macro for getting function arguments)
- Called from (representative examples):
  - No direct references found (likely called through SQL function dispatch)

## Notes and Other Information
- This function provides the backend implementation for PostgreSQL's array comparison operators
- Uses the existing array_cmp function which provides lexicographic ordering semantics
- Follows PostgreSQL's standard function calling convention for built-in functions
- The comparison semantics are consistent with other PostgreSQL array comparison operations
- Returns the input array unchanged (no copying), making it efficient for large arrays
- Complementary function to array_larger, implementing the opposite comparison logic