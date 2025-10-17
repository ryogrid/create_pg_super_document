# make_interval

## Location
[src/backend/utils/adt/timestamp.c:1539-1595](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1539-L1595)

## Overview
Creates an interval value from separate numeric components (years, months, weeks, days, hours, minutes, seconds), serving as the primary constructor function for PostgreSQL's INTERVAL type.

## Definition

```c
Datum
make_interval(PG_FUNCTION_ARGS)
```
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

## Simplified Source

```c
Datum make_interval(PG_FUNCTION_ARGS) {
    int32 years = PG_GETARG_INT32(0);
    int32 months = PG_GETARG_INT32(1);
    int32 weeks = PG_GETARG_INT32(2);
    int32 days = PG_GETARG_INT32(3);
    int32 hours = PG_GETARG_INT32(4);
    int32 mins = PG_GETARG_INT32(5);
    double secs = PG_GETARG_FLOAT8(6);

    // Check for invalid seconds input
    if (isinf(secs) || isnan(secs))
        goto out_of_range;

    Interval *result = (Interval *) palloc(sizeof(Interval));

    // Convert years and months to total months (with overflow check)
    if (pg_mul_s32_overflow(years, MONTHS_PER_YEAR, &result->month) ||
        pg_add_s32_overflow(result->month, months, &result->month))
        goto out_of_range;

    // Convert weeks and days to total days (with overflow check)
    if (pg_mul_s32_overflow(weeks, DAYS_PER_WEEK, &result->day) ||
        pg_add_s32_overflow(result->day, days, &result->day))
        goto out_of_range;

    // Convert hours and minutes to microseconds
    result->time = hours * USECS_PER_HOUR + mins * USECS_PER_MINUTE;

    // Convert seconds to microseconds and add to time
    secs = rint(float8_mul(secs, USECS_PER_SEC));
    if (!FLOAT8_FITS_IN_INT64(secs) ||
        pg_add_s64_overflow(result->time, (int64) secs, &result->time))
        goto out_of_range;

    // Ensure result is finite
    if (INTERVAL_NOT_FINITE(result))
        goto out_of_range;

    return PG_RETURN_INTERVAL_P(result);

out_of_range:
    ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
        errmsg("interval out of range")));
    return PG_RETURN_NULL();
}
```