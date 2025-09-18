# int8inc

## Location
[src/backend/utils/adt/int8.c:719-756](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L719-L756)

## Overview
Increments a 64-bit signed integer by 1, with specialized optimization for aggregate contexts and overflow detection.

## Definition
```c
Datum int8inc(PG_FUNCTION_ARGS)
```

## Detailed Description
This function increments a bigint (int64) value by 1. It includes an important optimization for aggregate operations like COUNT(): when called in an aggregate context and int8 is pass-by-reference, it performs in-place modification to avoid palloc overhead. For non-aggregate contexts or when int8 is pass-by-value, it uses the standard approach of creating a new result value.

The function implements overflow detection using pg_add_s64_overflow to ensure the increment operation doesn't exceed the valid range for bigint values.

## Parameters / Member Variables
- Function follows PostgreSQL's PG_FUNCTION_ARGS convention:
  - Single int64 argument extracted via PG_GETARG_INT64(0) or PG_GETARG_POINTER(0) depending on context

## Dependencies
- Functions called/Symbols referenced:
  - USE_FLOAT8_BYVAL (compilation flag that controls int8 behavior)
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (checks if called in aggregate context)
  - PG_GETARG_POINTER (pointer argument extraction for in-place modification)
  - PG_GETARG_INT64 (standard int64 argument extraction)
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md) (overflow-safe addition)
  - PG_RETURN_POINTER (return pointer for aggregate context)
  - PG_RETURN_INT64 (standard int64 return)
  - ereport/ERROR (error reporting)
- Called from (representative examples):
  - [int8inc_any](int8inc_any.md)
  - [int8inc_float8_float8](int8inc_float8_float8.md)

## Notes and Other Information
- Contains conditional compilation logic based on USE_FLOAT8_BYVAL flag
- Optimized for COUNT() aggregate operations through in-place modification when possible
- Always checks for overflow conditions and reports appropriate errors
- Uses two different code paths: optimized aggregate path and standard function path
- Located in src/backend/utils/adt/int8.c:719-756