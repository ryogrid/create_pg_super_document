# bpchar_pattern_lt

## Location
[src/backend/utils/adt/varchar.c:1141-1156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L1141-L1156)

## Overview
Implements the "less than" operator for pattern-based comparison of BPCHAR (blank-padded character) values, supporting character-by-character comparison suitable for LIKE clause indexing.

## Definition


## Detailed Description
This function performs a pattern-based "less than" comparison between two BPCHAR values. Unlike standard BPCHAR comparison which follows locale-specific collation rules, this function performs byte-by-byte comparison that is suitable for building indexes to support LIKE clauses and pattern matching operations.

The function extracts two BPCHAR arguments from the PostgreSQL function call interface, delegates the actual comparison logic to , and returns true if the first argument is lexicographically less than the second argument based on character-by-character comparison.

## Parameters / Member Variables
- : First BPCHAR value to compare (extracted from PG_FUNCTION_ARGS at position 0)
- : Second BPCHAR value to compare (extracted from PG_FUNCTION_ARGS at position 1)
- : Integer result from the internal comparison function

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
- The function is designed specifically for building indexes suitable for LIKE clauses, differing from locale-aware standard comparison operators