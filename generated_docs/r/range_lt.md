# range_lt

## Location
src/backend/utils/adt/rangetypes.c: 1295 - 1302

## Overview
PostgreSQL function that implements the "less than" comparison operator for range data types, returning true if the first range is less than the second range according to the B-tree ordering.

## Definition


## Detailed Description
The  function is one of PostgreSQL's inequality operators for range types. It provides the "<" operator functionality for range data types by utilizing the internal  comparison function. The function follows PostgreSQL's standard comparison semantics where empty ranges sort before all non-empty ranges, and non-empty ranges are compared first by their lower bounds, then by their upper bounds if the lower bounds are equal.

The function is implemented as a simple wrapper around , returning true (1) when the comparison result is negative (indicating the first range is less than the second), and false (0) otherwise.

## Parameters / Member Variables
- : Function call information structure containing the two range arguments to be compared

## Dependencies
- Functions called/Symbols referenced:
  - range_cmp (performs the actual range comparison logic)
- Called from (representative examples):
  - No direct references found in the codebase (likely used through operator dispatch)

## Notes and Other Information
- Part of PostgreSQL's B-tree support infrastructure for range types
- Implements the "<" operator for range data types
- Relies on range_cmp for the actual comparison logic, which handles empty ranges, type validation, and bound-by-bound comparison
- The function uses PostgreSQL's standard function calling convention with PG_FUNCTION_ARGS and PG_RETURN_BOOL macros
- Empty ranges are considered less than all non-empty ranges according to the underlying comparison logic