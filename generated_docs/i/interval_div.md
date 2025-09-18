# interval_div

## Location
src/backend/utils/adt/timestamp.c: 3697 - 3797

## Overview
A PostgreSQL function that implements interval division by a floating-point factor with comprehensive handling of special values and fractional unit cascading.

## Definition


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