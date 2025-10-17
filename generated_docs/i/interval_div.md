# interval_div

## Location
[src/backend/utils/adt/timestamp.c:3697-3797](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L3697-L3797)

## Overview
A PostgreSQL function that implements interval division by a floating-point factor with comprehensive handling of special values and fractional unit cascading.

## Definition

```c
Datum
interval_div(PG_FUNCTION_ARGS)
```
## Detailed Description
This function divides an interval by a floating-point factor, handling various edge cases including division by zero, NaN, and infinity conditions. The implementation follows similar logic to  but performs division instead of multiplication, with proper rounding and cascading of fractional units.

Key features:
- Explicit division by zero error checking
- Handles special values: NaN factors, infinite intervals, infinite factors
- Treats "infinity / infinity" as an error (no NaN equivalent in intervals)
- Division by infinity results in all fields being set to zero (handled by regular division)
- Cascades fractional parts from months to days to microseconds using the same approach as multiplication
- Uses TSROUND() for accurate floating-point calculations
- Includes overflow detection for all unit conversions

The division is performed component-wise on month, day, and time (microsecond) fields, with fractional remainders properly distributed to lower units using the same cascading logic as .

## Parameters / Member Variables
- Function uses  calling convention:
  - Argument 0: Interval to divide (dividend)
  - Argument 1: Floating-point division factor (divisor)
- Returns: Datum containing the resulting interval

## Dependencies
- Functions called/Symbols referenced:
  - ,  (argument extraction)
  -  (memory allocation)
  - ,  (special value detection)
  -  (infinite interval detection)
  -  (unary minus for intervals)
  - ,  (overflow checks)
  -  (timestamp rounding)
  -  (overflow-safe addition)
  -  (round to nearest integer)
  -  (absolute value)
  - , ,  (conversion constants)
  -  (return result)
  -  (error reporting)
- Called from (representative examples):
  -  (interval averaging function)

## Notes and Other Information
- This is a PostgreSQL V1 calling convention function, accessible from SQL as the '/' operator for intervals
- Explicit division by zero checking with ERRCODE_DIVISION_BY_ZERO error
- Uses the same fractional cascading approach as  (see comment reference to interval_mul)
- Fractional cascading flows downward: months→days→hours→minutes→seconds→microseconds
- Division by infinity naturally results in zero values through normal floating-point arithmetic
- Error handling for NaN, infinite operands, and overflow conditions
- Located in src/backend/utils/adt/timestamp.c:3697-3797

## Simplified Source

```c
Datum interval_div(PG_FUNCTION_ARGS) {
    // Extract arguments
    Interval *span = PG_GETARG_INTERVAL_P(0);
    float8 factor = PG_GETARG_FLOAT8(1);

    Interval *result = (Interval *) palloc(sizeof(Interval));

    // Check for division by zero
    if (factor == 0.0)
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
                       errmsg("division by zero")));

    // Handle special values (NaN, infinities)
    if (isnan(factor))
        goto out_of_range;

    if (INTERVAL_NOT_FINITE(span)) {
        if (isinf(factor))
            goto out_of_range;  // "infinity / infinity" is undefined

        if (factor < 0.0)
            interval_um_internal(span, result);  // negate
        else
            memcpy(result, span, sizeof(Interval));

        PG_RETURN_INTERVAL_P(result);
    }

    // Divide each component
    double result_double = span->month / factor;
    if (isnan(result_double) || !FLOAT8_FITS_IN_INT32(result_double))
        goto out_of_range;
    result->month = (int32) result_double;

    result_double = span->day / factor;
    if (isnan(result_double) || !FLOAT8_FITS_IN_INT32(result_double))
        goto out_of_range;
    result->day = (int32) result_double;

    // Handle fractional cascading (same logic as interval_mul)
    double month_remainder_days = (span->month / factor - result->month) * DAYS_PER_MONTH;
    month_remainder_days = TSROUND(month_remainder_days);

    double sec_remainder = (span->day / factor - result->day +
                           month_remainder_days - (int) month_remainder_days) * SECS_PER_DAY;
    sec_remainder = TSROUND(sec_remainder);

    // Handle overflow from seconds to days
    if (fabs(sec_remainder) >= SECS_PER_DAY) {
        if (pg_add_s32_overflow(result->day, (int)(sec_remainder / SECS_PER_DAY), &result->day))
            goto out_of_range;
        sec_remainder -= (int)(sec_remainder / SECS_PER_DAY) * SECS_PER_DAY;
    }

    // Add remainder days and compute final time
    if (pg_add_s32_overflow(result->day, (int32) month_remainder_days, &result->day))
        goto out_of_range;

    result_double = rint(span->time / factor + sec_remainder * USECS_PER_SEC);
    if (isnan(result_double) || !FLOAT8_FITS_IN_INT64(result_double))
        goto out_of_range;
    result->time = (int64) result_double;

    if (INTERVAL_NOT_FINITE(result))
        goto out_of_range;

    PG_RETURN_INTERVAL_P(result);

out_of_range:
    ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                   errmsg("interval out of range")));
    PG_RETURN_NULL();
}
```