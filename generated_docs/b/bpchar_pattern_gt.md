# bpchar_pattern_gt

## Location
src/backend/utils/adt/varchar.c: 1189 - 1204

## Overview
Implements the "greater than" operator for pattern-based comparison of BPCHAR (blank-padded character) values, supporting character-by-character comparison suitable for LIKE clause indexing.

## Definition
```c
Datum bpchar_pattern_gt(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a pattern-based "greater than" comparison between two BPCHAR values. It employs the same byte-by-byte comparison approach as other pattern comparison functions, ensuring compatibility with index operations designed to support LIKE clauses and pattern matching queries.

The function extracts two BPCHAR arguments from the PostgreSQL function call interface, uses `internal_bpchar_pattern_compare` for the comparison logic, and returns true if the first argument is lexicographically greater than the second argument.

## Parameters / Member Variables
- `arg1`: First BPCHAR value to compare (extracted from PG_FUNCTION_ARGS at position 0)
- `arg2`: Second BPCHAR value to compare (extracted from PG_FUNCTION_ARGS at position 1)
- `result`: Integer result from the internal comparison function

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BPCHAR_PP (macro to extract BPCHAR arguments)
  - [internal_bpchar_pattern_compare](../i/internal_bpchar_pattern_compare.md) (performs the actual comparison)
  - PG_FREE_IF_COPY (macro to free memory if needed)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - PostgreSQL query planner and executor for pattern-based comparisons
  - B-tree index operations for LIKE clause support

## Notes and Other Information
- This function is part of a family of pattern comparison operators that support character-by-character comparison
- The comparison is compatible with regular bpchareq/bpcharne operators and support functions when using "C" collation
- Memory management is handled through PG_FREE_IF_COPY macros to prevent memory leaks
- Returns true only when the comparison result is > 0, indicating the first argument is strictly greater than the second