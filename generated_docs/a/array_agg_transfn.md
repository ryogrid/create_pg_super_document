# array_agg_transfn

## Location
src/backend/utils/adt/array_userfuncs.c: 479 - 524

## Overview
PostgreSQL aggregate transition function that implements the ARRAY_AGG(anynonarray) aggregate, building an array by accumulating individual elements during aggregation.

## Definition
```c
Datum array_agg_transfn(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the transition function for the ARRAY_AGG aggregate, which collects individual values into an array during SQL aggregation operations. It maintains an ArrayBuildState structure that efficiently accumulates elements as they are processed by the aggregation engine.

The function handles the polymorphic nature of ARRAY_AGG by determining the element type at runtime and initializing the appropriate array building state. It processes each element by calling accumArrayResult, which handles the actual array construction and memory management.

The function is designed to work exclusively within PostgreSQL's aggregate execution framework and cannot be called directly due to its internal-type argument. It uses the aggregate memory context to ensure efficient memory management during aggregation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - Argument 0: Current aggregate state (ArrayBuildState pointer, can be null on first call)
  - Argument 1: Element to add to the array (can be null)

## Dependencies
- Functions called/Symbols referenced:
  - get_fn_expr_argtype
  - AggCheckCallContext
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_GETARG_DATUM
  - PG_RETURN_POINTER
  - initArrayResult
  - accumArrayResult
  - ereport
  - elog
- Called from (representative examples):
  - PostgreSQL aggregate execution engine during ARRAY_AGG processing
  - SQL queries using ARRAY_AGG aggregate function

## Notes and Other Information
- Designed exclusively for use within PostgreSQL's aggregate framework
- Uses "internal" transition type to pass ArrayBuildState pointer through nodeAgg.c
- Handles polymorphic aggregation by resolving element types at runtime
- Provides robust error handling for invalid calling contexts
- Efficiently manages memory using aggregate memory context
- Supports null element values in the resulting array
- Critical component of PostgreSQL's SQL aggregate functionality
- Parser validates array element type compatibility during query planning phase