# interval_mul

## Location
[src/backend/utils/adt/timestamp.c:3567-3686](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L3567-L3686)

## Overview
A PostgreSQL function that implements interval multiplication by a floating-point factor with comprehensive handling of special values and fractional unit cascading.

## Definition


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