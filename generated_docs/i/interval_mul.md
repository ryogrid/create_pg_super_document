# interval_mul

## Location
[src/backend/utils/adt/timestamp.c:3567-3686](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L3567-L3686)

## Overview
A PostgreSQL function that implements interval multiplication by a floating-point factor with comprehensive handling of special values and fractional unit cascading.

## Definition

```c
Datum
interval_mul(PG_FUNCTION_ARGS)
```
## Detailed Description
This function multiplies an interval by a floating-point factor, handling various edge cases including NaN, infinities, and overflow conditions. The implementation uses careful floating-point arithmetic with proper rounding and cascading of fractional units from higher to lower precision units.

Key features:
- Handles special values: NaN factors, infinite intervals, infinite factors
- Treats "0 * infinity" and "infinity * 0" as errors (no NaN equivalent in intervals)
- Cascades fractional parts from months to days to microseconds using conversion factors
- Uses TSROUND() for more accurate results in floating-point calculations
- Includes overflow detection for all unit conversions
- Does not cascade upward (e.g., hours to days) - user can use justify_hours/justify_days later

The multiplication is performed component-wise on month, day, and time (microsecond) fields, with fractional parts properly distributed to lower units.

## Parameters / Member Variables
- Function uses  calling convention:
  - Argument 0: Interval to multiply
  - Argument 1: Floating-point multiplication factor
- Returns: Datum containing the resulting interval

## Dependencies
- Functions called/Symbols referenced:
  - ,  (argument extraction)
  -  (memory allocation)
  - ,  (special value detection)
  -  (infinite interval detection)
  -  (unary minus for intervals)
  -  (determine interval sign)
  - ,  (set infinite values)
  - ,  (overflow checks)
  -  (timestamp rounding)
  -  (overflow-safe addition)
  -  (round to nearest integer)
  - , ,  (conversion constants)
  -  (return result)
- Called from (representative examples):
  -  (interval linear interpolation)
  -  (reverse order multiplication wrapper)

## Notes and Other Information
- This is a PostgreSQL V1 calling convention function, accessible from SQL as the '*' operator for intervals
- Implements careful floating-point arithmetic to minimize precision errors
- Fractional cascading flows downward: months→days→hours→minutes→seconds→microseconds
- Uses conversion factors: DAYS_PER_MONTH (30), SECS_PER_DAY (86400), USECS_PER_SEC (1000000)
- Error handling for NaN, infinite operands, and overflow conditions
- No interval absolute value function exists due to ambiguity in what value to return
- Located in src/backend/utils/adt/timestamp.c:3567-3686

## Simplified Source

```c
Datum interval_mul(PG_FUNCTION_ARGS) {
    // Extract arguments
    Interval *span = PG_GETARG_INTERVAL_P(0);
    float8 factor = PG_GETARG_FLOAT8(1);

    Interval *result = (Interval *) palloc(sizeof(Interval));

    // Handle special values (NaN, infinities)
    if (isnan(factor))
        goto out_of_range;

    if (INTERVAL_NOT_FINITE(span)) {
        if (factor == 0.0)
            goto out_of_range;  // "infinity * 0" is undefined

        if (factor < 0.0)
            interval_um_internal(span, result);  // negate
        else
            memcpy(result, span, sizeof(Interval));

        PG_RETURN_INTERVAL_P(result);
    }

    if (isinf(factor)) {
        int isign = interval_sign(span);
        if (isign == 0)
            goto out_of_range;  // "0 * infinity" is undefined

        if (factor * isign < 0)
            INTERVAL_NOBEGIN(result);
        else
            INTERVAL_NOEND(result);

        PG_RETURN_INTERVAL_P(result);
    }

    // Multiply each component
    double result_double = span->month * factor;
    if (isnan(result_double) || !FLOAT8_FITS_IN_INT32(result_double))
        goto out_of_range;
    result->month = (int32) result_double;

    result_double = span->day * factor;
    if (isnan(result_double) || !FLOAT8_FITS_IN_INT32(result_double))
        goto out_of_range;
    result->day = (int32) result_double;

    // Handle fractional cascading from months to days to microseconds
    double month_remainder_days = (span->month * factor - result->month) * DAYS_PER_MONTH;
    month_remainder_days = TSROUND(month_remainder_days);

    double sec_remainder = (span->day * factor - result->day +
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

    result_double = rint(span->time * factor + sec_remainder * USECS_PER_SEC);
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