# make_interval

## Location
src/backend/utils/adt/timestamp.c: 1539 - 1595

## Overview
Creates an interval value from separate numeric components (years, months, weeks, days, hours, minutes, seconds), serving as the primary constructor function for PostgreSQL's INTERVAL type.

## Definition


## Detailed Description
This PostgreSQL built-in function constructs an Interval data structure from individual numeric parameters. It processes each time component separately and combines them into a single interval representation using PostgreSQL's internal microsecond-based storage format.

The function performs comprehensive overflow checking at each step to ensure the resulting interval remains within valid bounds. It handles the conversion of floating-point seconds to microseconds with proper rounding, and validates that the final result represents a finite interval value.

All input validation is performed with overflow-safe arithmetic operations, and any out-of-range inputs trigger an error with the ERRCODE_DATETIME_VALUE_OUT_OF_RANGE error code.

## Parameters / Member Variables
- : 32-bit integer representing the year component
- : 32-bit integer representing the month component  
- : 32-bit integer representing the week component
- : 32-bit integer representing the day component
- : 32-bit integer representing the hour component
- : 32-bit integer representing the minute component
- : Double-precision floating-point representing the seconds component (including fractional seconds)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32, PG_GETARG_FLOAT8 (argument extraction macros)
  - isinf, isnan (IEEE floating-point checks)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - [pg_mul_s32_overflow](../p/pg_mul_s32_overflow.md), pg_add_s32_overflow, pg_add_s64_overflow (overflow-safe arithmetic)
  - [float8_mul](../f/float8_mul.md), rint (floating-point operations)
  - FLOAT8_FITS_IN_INT64 (range validation macro)
  - INTERVAL_NOT_FINITE (infinity check macro)
  - PG_RETURN_INTERVAL_P (return value macro)
  - ereport (error reporting)
- Called from:
  - SQL function calls to make_interval()

## Notes and Other Information
- Available as a SQL function: make_interval(years, months, weeks, days, hours, mins, secs)
- Uses microsecond precision internally for time components
- Performs strict overflow checking to prevent integer wraparound
- Rejects infinite or NaN seconds input values
- Combines weeks into days (weeks * 7) and years into months (years * 12) during processing
- Function signature follows PostgreSQL's PG_FUNCTION_ARGS convention for SQL-callable functions