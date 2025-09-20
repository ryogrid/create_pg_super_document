# dtoi4

## Location
[src/backend/utils/adt/float.c:1207-1231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1207-L1231)

## Overview
A conversion function that converts a float8 (double precision) number to an int4 (32-bit integer) with range checking and fractional part handling.

## Definition

```c
Datum
dtoi4(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs type conversion from PostgreSQL's float8 data type (double precision floating point) to int4 data type (32-bit signed integer). The conversion process includes several important steps: first, any fractional part is removed using rint() to avoid failing on values that would round into the valid range; second, comprehensive range checking ensures the resulting value fits within the 32-bit signed integer range; and finally, NaN values are explicitly rejected. The function is designed to be robust against edge cases while providing predictable conversion behavior.

## Parameters / Member Variables
-  (float8): The double-precision floating-point number to be converted to a 32-bit integer

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (double-precision argument extraction macro)
  - rint (round to nearest integer function)
  - isnan (NaN detection function)
  - FLOAT8_FITS_IN_INT32 (macro to check if float8 value fits in int32 range)
  - ereport (error reporting function)
  - PG_RETURN_INT32 (32-bit integer return macro)
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's type conversion system)

## Notes and Other Information
- Uses rint() to eliminate fractional parts before range checking, allowing values like 2147483647.9 to convert successfully
- Performs comprehensive range validation using FLOAT8_FITS_IN_INT32 macro
- Rejects NaN values explicitly with ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
- Part of PostgreSQL's floating-point to integer conversion infrastructure
- The rint() function preserves NaN and infinity values unchanged for proper error handling
- More complex than simple casting due to the need for proper range checking and fraction handling
- Source location: src/backend/utils/adt/float.c:1207-1231