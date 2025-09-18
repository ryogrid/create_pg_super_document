# timestamp_support

## Location
src/backend/utils/adt/timestamp.c: 325 - 344

## Overview
Provides planner support for timestamp scale coercion functions, enabling query optimization for timestamp precision conversions.

## Definition
```c
Datum timestamp_support(PG_FUNCTION_ARGS)
```

## Detailed Description
The `timestamp_support` function serves as a planner support function for PostgreSQL's query optimizer when dealing with timestamp scale coercion operations. It specifically supports both `timestamp_scale()` and `timestamptz_scale()` functions, helping the planner optimize queries that involve precision changes in timestamp values.

The function handles `SupportRequestSimplify` requests by delegating to `TemporalSimplify` with the maximum timestamp precision constraint. This allows the planner to potentially simplify or optimize expressions involving timestamp precision coercions, such as eliminating redundant scale operations or folding constants.

The support function is part of PostgreSQL's extensible type system, allowing data types to provide hints to the query planner for better optimization of operations involving those types.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]` (Node *rawreq): Support request from the planner, typically a SupportRequestSimplify structure

## Dependencies
- Functions called/Symbols referenced:
  - SupportRequestSimplify
  - TemporalSimplify
  - MAX_TIMESTAMP_PRECISION
- Called from (representative examples):
  - No direct references found in the current analysis

## Notes and Other Information
- This function supports both timestamp and timestamptz scale operations without distinction
- Part of PostgreSQL's planner support infrastructure for advanced query optimization
- Handles simplification requests to potentially optimize timestamp precision coercion expressions
- Uses MAX_TIMESTAMP_PRECISION as the upper bound for precision specifications
- Returns NULL for unsupported request types, allowing for future extensibility
- Located in src/backend/utils/adt/timestamp.c:325-344