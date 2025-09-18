# btbpchar_pattern_cmp

## Location
src/backend/utils/adt/varchar.c: 1205 - 1220

## Overview
Implements the comparison support function for B-tree indexes on BPCHAR (blank-padded character) values using pattern-based comparison, essential for creating indexes that support LIKE clause operations.

## Definition
```c
Datum btbpchar_pattern_cmp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the comparison support function for B-tree indexes on BPCHAR data types when pattern-based ordering is required. Unlike the boolean comparison operators (lt, le, ge, gt), this function returns the actual comparison result as an integer, which is essential for B-tree index construction and maintenance.

The function performs character-by-character comparison of two BPCHAR values and returns a negative value if the first argument is less than the second, zero if they are equal, and a positive value if the first argument is greater than the second. This three-way comparison result enables efficient B-tree operations for pattern matching queries.

## Parameters / Member Variables
- `arg1`: First BPCHAR value to compare (extracted from PG_FUNCTION_ARGS at position 0)
- `arg2`: Second BPCHAR value to compare (extracted from PG_FUNCTION_ARGS at position 1)
- `result`: Integer comparison result from the internal comparison function

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BPCHAR_PP (macro to extract BPCHAR arguments)
  - internal_bpchar_pattern_compare (performs the actual comparison)
  - PG_FREE_IF_COPY (macro to free memory if needed)
  - PG_RETURN_INT32 (macro to return 32-bit integer result)
- Called from (representative examples):
  - B-tree index creation and maintenance operations
  - PostgreSQL index access methods for pattern-based queries
  - Query planner for index selection with LIKE clauses

## Notes and Other Information
- This function is the core comparison function used by B-tree indexes for pattern-based BPCHAR ordering
- Returns the raw comparison result (-1, 0, or 1) rather than a boolean value like the other pattern comparison functions
- Essential for building indexes that can efficiently support LIKE pattern matching queries
- The comparison is compatible with "C" collation and character-by-character comparison semantics
- Memory management follows PostgreSQL conventions with PG_FREE_IF_COPY for proper cleanup