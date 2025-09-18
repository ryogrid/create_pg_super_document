# bpchargt

## Location
src/backend/utils/adt/varchar.c: 875 - 895

## Overview
This function implements the "greater than" comparison operator (>) for the BpChar data type (blank-padded character strings) in PostgreSQL.

## Definition
```c
Datum bpchargt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `bpchargt` function compares two BpChar values and returns true if the first argument is greater than the second argument. It performs a collation-aware string comparison after determining the true length of both strings (excluding trailing spaces). The function uses PostgreSQL's standard variable-length string comparison logic while respecting the current collation settings.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro to access function arguments:
  - `arg1`: First BpChar value to compare (left operand)
  - `arg2`: Second BpChar value to compare (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_BPCHAR_PP` - Extract BpChar arguments from function call
  - `[bcTruelen](bcTruelen.md)` - Calculate true length of BpChar (excluding trailing spaces)
  - `[varstr_cmp](../v/varstr_cmp.md)` - Perform collation-aware string comparison
  - `VARDATA_ANY` - Get pointer to variable-length data
  - `PG_GET_COLLATION` - Get current collation for comparison
  - `PG_FREE_IF_COPY` - Free memory if argument was copied
  - `PG_RETURN_BOOL` - Return boolean result
- Called from (representative examples):
  - No direct references found in the codebase (likely used through operator system)

## Notes and Other Information
- This function is typically invoked through the > operator for BpChar types
- The comparison is collation-aware, respecting locale-specific sorting rules
- Memory management is handled properly with PG_FREE_IF_COPY calls
- Returns true if the comparison result is > 0 (first string is greater than second)
- Part of PostgreSQL's type system for CHAR(n) data type operations