# array_agg_array_transfn

## Location
src/backend/utils/adt/array_userfuncs.c: 857 - 900

## Overview
Transition function for the ARRAY_AGG(anyarray) aggregate that accumulates array inputs into a single result array.

## Definition


## Detailed Description
This function serves as the transition function for the ARRAY_AGG() aggregate when operating on array inputs (as opposed to scalar inputs). It accumulates individual arrays into an ArrayBuildStateArr structure, which maintains state for building multi-dimensional result arrays. The function handles the initialization of state on the first call and subsequent accumulation of array values.

Unlike the scalar version of array_agg that builds a simple array from individual elements, this variant concatenates or combines arrays into higher-dimensional structures. It validates the input array type at runtime and manages memory allocation within the aggregate context.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : ArrayBuildStateArr pointer for maintaining aggregate state (may be NULL on first call)
  - : Array datum to be accumulated into the result

## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md)
  - [ArrayBuildStateArr](../A/ArrayBuildStateArr.md)
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - initArrayResultArr
  - accumArrayResultArr
  - ereport
  - elog
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_GETARG_DATUM
  - PG_RETURN_POINTER
- Called from (representative examples):
  - PostgreSQL aggregate framework (internal)
  - ARRAY_AGG(anyarray) aggregate function execution

## Notes and Other Information
- Specifically designed for array-to-array aggregation scenarios
- Different from the scalar array_agg_transfn which handles individual elements
- Validates input array types at runtime using get_fn_expr_argtype
- Uses ArrayBuildStateArr instead of ArrayBuildState for array-specific state management
- The transition type is declared as "internal" to pass pointers through nodeAgg.c
- Parser validation ensures input types are valid arrays before runtime execution
- Memory management handled within the aggregate context for proper cleanup
- Essential for enabling ARRAY_AGG to work with array inputs rather than just scalar values