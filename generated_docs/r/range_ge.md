# range_ge

## Location
src/backend/utils/adt/rangetypes.c: 1311 - 1318

## Overview
PostgreSQL function that implements the "greater than or equal to" comparison operator for range data types, returning true if the first range is greater than or equal to the second range.

## Definition
```c
Datum range_ge(PG_FUNCTION_ARGS)
```

## Detailed Description
The `range_ge` function implements the ">=" operator for PostgreSQL range data types. It serves as a wrapper around the `range_cmp` function, returning true when the comparison result is greater than or equal to zero, indicating that the first range is either greater than or equal to the second range according to B-tree ordering semantics.

The function follows the same comparison rules as other range comparison operators: empty ranges sort before all non-empty ranges, and non-empty ranges are compared lexicographically by their bounds (lower bound first, then upper bound if lower bounds are equal).

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the two range arguments to be compared

## Dependencies
- Functions called/Symbols referenced:
  - range_cmp (performs the actual range comparison logic)
- Called from (representative examples):
  - No direct references found in the codebase (likely used through operator dispatch)

## Notes and Other Information
- Part of PostgreSQL's B-tree support infrastructure for range types
- Implements the ">=" operator for range data types
- Returns true for both "greater than" and "equal" comparison results from range_cmp
- Uses PostgreSQL's standard function calling convention with PG_FUNCTION_ARGS and PG_RETURN_BOOL macros
- Provides the inverse comparison logic to range_lt by accepting zero and positive comparison results