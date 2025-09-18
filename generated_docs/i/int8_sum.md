# int8_sum

## Location
src/backend/utils/adt/numeric.c: 6625 - 6665

## Overview
An obsolete SQL aggregate transition function that computes the sum of bigint (int8) values using Numeric arithmetic, no longer used for SUM(int8) operations.

## Definition
```c
Datum int8_sum(PG_FUNCTION_ARGS)
```

## Detailed Description
This function was originally designed to serve as the state transition function for the SUM() aggregate when applied to bigint (int8) data. Unlike int2_sum and int4_sum which use wider integer accumulators, int8_sum uses PostgreSQL's arbitrary-precision Numeric type to prevent overflow. However, the function is explicitly marked as obsolete and is no longer used in current PostgreSQL versions for SUM(int8) operations.

The function converts int8 values to Numeric format and uses numeric_add for the arithmetic operations. Unlike its integer counterparts, it cannot perform in-place optimization because Numeric values are variable-sized.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing:
  - Argument 0: Current accumulator state (Numeric, initially NULL)
  - Argument 1: New int8 value to add to sum

## Dependencies
- Functions called/Symbols referenced:
  - int64_to_numeric (convert int8 to Numeric)
  - numeric_add (add two Numeric values)
  - DirectFunctionCall2 (direct function call mechanism)
  - NumericGetDatum (convert Numeric to Datum)
  - PG_ARGISNULL (null checking macros)
  - PG_GETARG_INT64 (extract int8 argument)
  - PG_GETARG_NUMERIC (extract Numeric argument)
  - PG_RETURN_NULL (return null result)
  - PG_RETURN_NUMERIC (return Numeric result)
  - PG_RETURN_DATUM (return Datum result)
- Called from:
  - No references found (obsolete function)

## Notes and Other Information
- Explicitly marked as obsolete in the source code comments
- No longer used for SUM(int8) operations in current PostgreSQL versions
- Cannot use in-place optimization like int2_sum and int4_sum due to variable-sized Numeric format
- Uses arbitrary-precision Numeric arithmetic to handle potential overflow from int8 summation
- Maintained in the codebase for backward compatibility or historical reasons
- Located in src/backend/utils/adt/numeric.c:6625-6665