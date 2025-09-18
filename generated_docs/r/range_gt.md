# range_gt

## Location
src/backend/utils/adt/rangetypes.c: 1319 - 1329

## Overview
PostgreSQL function that implements the "greater than" comparison operator for range data types, returning true if the first range is greater than the second range according to B-tree ordering.

## Definition
```c
Datum range_gt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `range_gt` function implements the ">" operator for PostgreSQL range data types. It serves as a wrapper around the `range_cmp` function, returning true only when the comparison result is strictly positive, indicating that the first range is greater than the second range according to B-tree ordering semantics.

The function follows PostgreSQL's standard range comparison rules: empty ranges sort before all non-empty ranges, and non-empty ranges are compared lexicographically by their bounds (lower bound first, then upper bound if necessary). This makes `range_gt` the logical inverse of `range_le`.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the two range arguments to be compared

## Dependencies
- Functions called/Symbols referenced:
  - range_cmp (performs the actual range comparison logic)
- Called from (representative examples):
  - No direct references found in the codebase (likely used through operator dispatch)

## Notes and Other Information
- Part of PostgreSQL's B-tree support infrastructure for range types
- Implements the ">" operator for range data types
- Returns true only for strictly positive comparison results from range_cmp (excludes equality)
- Uses PostgreSQL's standard function calling convention with PG_FUNCTION_ARGS and PG_RETURN_BOOL macros
- Provides the inverse comparison logic to range_le by accepting only positive comparison results