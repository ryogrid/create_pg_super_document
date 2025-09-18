# array_larger

## Location
[src/backend/utils/adt/arrayfuncs.c:5875-5883](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5875-L5883)

## Overview
array_larger returns the lexicographically larger of two arrays by comparing them using array comparison semantics.

## Definition
```c
Datum
array_larger(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL GREATEST function for arrays by performing lexicographic comparison using array_cmp. It returns the first array if it is lexicographically greater than the second array, otherwise it returns the second array. The comparison follows PostgreSQL's standard array comparison rules, comparing elements pairwise from left to right.

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