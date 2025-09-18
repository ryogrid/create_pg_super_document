# int4_sum

## Location
src/backend/utils/adt/numeric.c: 6573 - 6624

## Overview
A SQL aggregate transition function that computes the sum of integer (int4) values, using int8 accumulator to prevent overflow.

## Definition
```c
Datum int4_sum(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the state transition function for the SUM() aggregate when applied to integer (int4) data. Similar to int2_sum, it uses a wider int8 (64-bit) accumulator to prevent overflow that could occur with int4 arithmetic. The function handles the SQL requirement that SUM() of no values returns NULL by explicitly managing null states.

The function is non-strict and must handle null inputs explicitly. When called in aggregate context, it optimizes performance by modifying the accumulator in-place rather than allocating new memory, provided the platform supports pass-by-reference for int8 values.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing:
  - Argument 0: Current accumulator state (int8*, initially NULL)  
  - Argument 1: New int4 value to add to sum

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL (null checking macros)
  - PG_GETARG_INT32 (extract int4 argument)
  - PG_GETARG_INT64 (extract int8 argument)
  - PG_GETARG_POINTER (extract pointer argument)
  - PG_RETURN_NULL (return null result)
  - PG_RETURN_INT64 (return int8 result)  
  - PG_RETURN_POINTER (return pointer result)
  - AggCheckCallContext (check if called in aggregate context)
- Called from:
  - No direct references found (called through PostgreSQL's aggregate mechanism)

## Notes and Other Information
- Part of PostgreSQL's integer sum aggregation system, nearly identical to int2_sum except it processes int4 inputs
- Uses conditional compilation (USE_FLOAT8_BYVAL) to optimize for different architectures
- The in-place modification optimization is only available when int8 is pass-by-reference
- Handles the first non-null input specially since the initial state is NULL
- Only used in plain aggregation mode; moving-aggregate mode uses different functions
- Despite being for integer types, it's located in numeric.c alongside other aggregation functions
- Located in src/backend/utils/adt/numeric.c:6573-6624