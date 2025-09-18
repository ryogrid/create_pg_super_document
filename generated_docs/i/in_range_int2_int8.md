# in_range_int2_int8

## Location
src/backend/utils/adt/int.c: 751 - 770

## Overview
A PostgreSQL function that determines whether a given int2 value falls within a range defined by a base int2 value and an int8 offset.

## Definition
```c
Datum in_range_int2_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function determines if a value of type int2 (smallint) is within a specified range where the offset is of type int8 (bigint). To avoid code duplication, it delegates the actual range checking to the `in_range_int4_int8` function by converting both int2 parameters (the test value and base value) to int4 (int32) format. This design leverages existing range checking logic while maintaining proper type conversion and validation.

The function uses PostgreSQL's function call interface and returns a Datum result indicating whether the value falls within the specified range boundaries.

## Parameters / Member Variables
- `PG_GETARG_INT16(0)`: The value to test (int2), converted to int32 for the delegated call
- `PG_GETARG_INT16(1)`: The base value of the range (int2), converted to int32 for the delegated call
- `PG_GETARG_DATUM(2)`: The offset value (int8)
- `PG_GETARG_DATUM(3)`: Boolean flag indicating if the range includes the lower bound
- `PG_GETARG_DATUM(4)`: Boolean flag indicating if the range includes the upper bound

## Dependencies
- Functions called/Symbols referenced:
  - in_range_int4_int8
  - DirectFunctionCall5
  - PG_GETARG_INT16
  - Int32GetDatum
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:751-770
- Demonstrates PostgreSQL's systematic approach to providing range functions for all type combinations
- The conversion from int2 to int4 for both test and base values ensures compatibility with the underlying int4_int8 range logic
- Part of the comprehensive matrix of range checking functions that PostgreSQL provides for window functions and other range-based operations