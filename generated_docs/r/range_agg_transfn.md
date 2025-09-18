# range_agg_transfn

## Location
src/backend/utils/adt/multirangetypes.c: 1340 - 1371

## Overview
The transition function for the range_agg aggregate that collects input range values into an array for later processing by the finalize function.

## Definition
Datum range_agg_transfn(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the transition function for the range_agg aggregate operation in PostgreSQL. Its primary role is to accumulate range values during the aggregation process. Rather than performing any range operations directly, it simply collects all non-null range inputs into an ArrayBuildState structure.

The function validates that it's being called in a proper aggregate context and ensures the input is actually a range type. It skips NULL values and accumulates valid range values in the aggregate context's memory. The actual combining of adjacent/overlapping ranges is deferred to the finalfn (range_agg_finalfn).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call interface containing:
  - Argument 0: Current aggregate state (ArrayBuildState pointer, nullable on first call)
  - Argument 1: New range value to add to the aggregate

## Dependencies
- Functions called/Symbols referenced:
  - AggCheckCallContext
  - get_fn_expr_argtype
  - type_is_range
  - initArrayResult
  - accumArrayResult
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_GETARG_DATUM
  - PG_RETURN_POINTER
  - elog
- Called from:
  - No direct callers found (called through SQL aggregate function interface)

## Notes and Other Information
- Part of PostgreSQL's range aggregation system
- Does not perform range combination logic - only collects inputs
- Validates aggregate context and range type inputs
- Skips NULL input values
- Memory allocation occurs in the aggregate context for proper cleanup
- Works in conjunction with range_agg_finalfn to implement the complete range_agg aggregate
- Located in src/backend/utils/adt/multirangetypes.c:1340-1371