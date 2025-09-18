# int8dec

## Location
src/backend/utils/adt/int8.c: 757 - 803

## Overview
Decrements a 64-bit signed integer by 1, with specialized optimization for aggregate contexts and overflow detection.

## Definition
```c
Datum int8dec(PG_FUNCTION_ARGS)
```

## Detailed Description
This function decrements a bigint (int64) value by 1. Like int8inc, it includes an important optimization for aggregate operations: when called in an aggregate context and int8 is pass-by-reference, it performs in-place modification to avoid palloc overhead. For non-aggregate contexts or when int8 is pass-by-value, it uses the standard approach of creating a new result value.

The function implements underflow detection using pg_sub_s64_overflow to ensure the decrement operation doesn't go below the valid range for bigint values.

## Parameters / Member Variables
- Function follows PostgreSQL's PG_FUNCTION_ARGS convention:
  - Single int64 argument extracted via PG_GETARG_INT64(0) or PG_GETARG_POINTER(0) depending on context

## Dependencies
- Functions called/Symbols referenced:
  - USE_FLOAT8_BYVAL (compilation flag that controls int8 behavior)
  - AggCheckCallContext (checks if called in aggregate context)
  - PG_GETARG_POINTER (pointer argument extraction for in-place modification)
  - PG_GETARG_INT64 (standard int64 argument extraction)
  - pg_sub_s64_overflow (overflow-safe subtraction)
  - PG_RETURN_POINTER (return pointer for aggregate context)
  - PG_RETURN_INT64 (standard int64 return)
  - ereport/ERROR (error reporting)
- Called from (representative examples):
  - int8dec_any

## Notes and Other Information
- Contains conditional compilation logic based on USE_FLOAT8_BYVAL flag
- Mirror function to int8inc, providing decrement functionality
- Optimized for aggregate operations through in-place modification when possible
- Always checks for underflow conditions and reports appropriate errors
- Uses two different code paths: optimized aggregate path and standard function path
- Located in src/backend/utils/adt/int8.c:757-803