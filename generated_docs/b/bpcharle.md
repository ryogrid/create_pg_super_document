# bpcharle

## Location
[src/backend/utils/adt/varchar.c:854-874](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L854-L874)

## Overview
This function implements the "less than or equal to" comparison operator (<=) for the BpChar data type (blank-padded character strings) in PostgreSQL.

## Definition


## Detailed Description
The  function compares two BpChar values and returns true if the first argument is less than or equal to the second argument. It performs a collation-aware string comparison after determining the true length of both strings (excluding trailing spaces). The function uses PostgreSQL's standard variable-length string comparison logic while respecting the current collation settings.

## Parameters / Member Variables
- Uses  macro to access function arguments:
  - : First BpChar value to compare (left operand)
  - : Second BpChar value to compare (right operand)

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract BpChar arguments from function call
  -  - Calculate true length of BpChar (excluding trailing spaces)
  -  - Perform collation-aware string comparison
  -  - Get pointer to variable-length data
  -  - Get current collation for comparison
  -  - Free memory if argument was copied
  -  - Return boolean result
- Called from (representative examples):
  - No direct references found in the codebase (likely used through operator system)

## Notes and Other Information
- This function is typically invoked through the <= operator for BpChar types
- The comparison is collation-aware, respecting locale-specific sorting rules
- Memory management is handled properly with PG_FREE_IF_COPY calls
- Returns true if the comparison result is <= 0 (first string is less than or equal to second)
- Part of PostgreSQL's type system for CHAR(n) data type operations