# multirange_agg_transfn

## Location
src/backend/utils/adt/multirangetypes.c: 1412 - 1464

## Overview
The transition function for the multirange_agg aggregate that decomposes input multiranges into individual ranges and collects them for later processing by the finalize function.

## Definition
Datum multirange_agg_transfn(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the transition function for the multirange_agg aggregate operation in PostgreSQL. Unlike range_agg_transfn which works with individual ranges, this function handles multirange inputs by decomposing each multirange into its constituent ranges and accumulating all individual ranges in an ArrayBuildState.

The function validates that it's being called in a proper aggregate context and ensures the input is actually a multirange type. For each non-null multirange input, it deserializes the multirange to extract individual ranges and adds each range to the accumulation array. Special handling is provided for empty multiranges to ensure an empty result rather than a null result.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call interface containing:
  - Argument 0: Current aggregate state (ArrayBuildState pointer, nullable on first call)
  - Argument 1: New multirange value to decompose and add to the aggregate

## Dependencies
- Functions called/Symbols referenced:
  - AggCheckCallContext
  - get_fn_expr_argtype
  - type_is_multirange
  - multirange_get_typcache
  - initArrayResult
  - PG_GETARG_MULTIRANGE_P
  - multirange_deserialize
  - accumArrayResult
  - make_empty_range
  - RangeTypePGetDatum
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_RETURN_POINTER
  - elog
- Called from:
  - No direct callers found (called through SQL aggregate function interface)

## Notes and Other Information
- Part of PostgreSQL's multirange aggregation system
- Decomposes multiranges into individual ranges for accumulation
- Validates aggregate context and multirange type inputs
- Skips NULL input values
- Handles empty multiranges specially by adding an empty range to maintain proper semantics
- Memory allocation occurs in the aggregate context for proper cleanup
- Works in conjunction with a finalize function to implement the complete multirange_agg aggregate
- Accumulates individual ranges from all input multiranges for later merging
- Located in src/backend/utils/adt/multirangetypes.c:1412-1464