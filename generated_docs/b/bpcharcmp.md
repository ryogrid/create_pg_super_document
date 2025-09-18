# bpcharcmp

## Location
src/backend/utils/adt/varchar.c: 917 - 937

## Overview
This function implements the three-way comparison operation for the BpChar data type (blank-padded character strings) in PostgreSQL, returning an integer indicating the relative ordering of two values.

## Definition
```c
Datum bpcharcmp(PG_FUNCTION_ARGS)
```

## Detailed Description
The `bpcharcmp` function performs a three-way comparison between two BpChar values and returns an integer result: negative if the first argument is less than the second, zero if they are equal, and positive if the first argument is greater than the second. It performs a collation-aware string comparison after determining the true length of both strings (excluding trailing spaces). This function is fundamental to PostgreSQL's sorting and indexing operations for BpChar types.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro to access function arguments:
  - `arg1`: First BpChar value to compare (left operand)
  - `arg2`: Second BpChar value to compare (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_BPCHAR_PP` - Extract BpChar arguments from function call
  - [bcTruelen](bcTruelen.md) - Calculate true length of BpChar (excluding trailing spaces)
  - [varstr_cmp](../v/varstr_cmp.md) - Perform collation-aware string comparison
  - `VARDATA_ANY` - Get pointer to variable-length data
  - `PG_GET_COLLATION` - Get current collation for comparison
  - `PG_FREE_IF_COPY` - Free memory if argument was copied
  - `PG_RETURN_INT32` - Return integer result
- Called from (representative examples):
  - No direct references found in the codebase (likely used through operator system)

## Notes and Other Information
- This is the core comparison function used by PostgreSQL's B-tree indexing and sorting operations
- Returns negative, zero, or positive integer for less than, equal to, or greater than relationships
- The comparison is collation-aware, respecting locale-specific sorting rules
- Memory management is handled properly with PG_FREE_IF_COPY calls
- Part of PostgreSQL's type system for CHAR(n) data type operations
- Used internally by other comparison operators and sorting algorithms