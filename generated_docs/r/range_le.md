# range_le

## Location
src/backend/utils/adt/rangetypes.c: 1303 - 1310

## Overview
PostgreSQL function that implements the "less than or equal to" comparison operator for range data types, returning true if the first range is less than or equal to the second range.

## Definition
```c
Datum range_le(PG_FUNCTION_ARGS)
```

## Detailed Description
The `range_le` function implements the "<=" operator for PostgreSQL range data types. Like `range_lt`, it serves as a wrapper around the `range_cmp` function but returns true when the comparison result is less than or equal to zero, meaning the first range is either less than or equal to the second range according to B-tree ordering semantics.

The function follows PostgreSQL's range comparison rules where empty ranges sort before all non-empty ranges, and non-empty ranges are compared by their bounds in lexicographic order (lower bound first, then upper bound).

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the two range arguments to be compared

## Dependencies
- Functions called/Symbols referenced:
  - range_cmp (performs the actual range comparison logic)
- Called from (representative examples):
  - No direct references found in the codebase (likely used through operator dispatch)

## Notes and Other Information
- Part of PostgreSQL's B-tree support infrastructure for range types
- Implements the "<=" operator for range data types
- Returns true for both "less than" and "equal" comparison results from range_cmp
- Uses PostgreSQL's standard function calling convention with PG_FUNCTION_ARGS and PG_RETURN_BOOL macros
- Complements the range_lt function by including equality in the comparison result