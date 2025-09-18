# finite_interval_mi

## Location
src/backend/utils/adt/timestamp.c: 3503 - 3517

## Overview
A static helper function that performs safe subtraction between two finite intervals with overflow detection and validation.

## Definition


## Detailed Description
This function performs interval subtraction (span1 - span2) with comprehensive safety checks. It validates that both input intervals are finite and performs overflow-safe arithmetic on each component (month, day, time). The function ensures that the resulting interval remains within valid bounds and raises an error if any overflow occurs or if the result becomes infinite.

The subtraction is performed component-wise:
- Month components are subtracted using overflow-safe integer subtraction
- Day components are subtracted using overflow-safe integer subtraction  
- Time components (microseconds) are subtracted using overflow-safe 64-bit integer subtraction

## Parameters / Member Variables
- : The first interval (minuend) - must be finite
- : The second interval (subtrahend) - must be finite
- : Output parameter to store the computed difference interval

## Dependencies
- Functions called/Symbols referenced:
  -  (macro for checking infinite intervals)
  -  (overflow-safe 32-bit subtraction)
  -  (overflow-safe 64-bit subtraction)
  -  (error reporting)
- Called from (representative examples):
  -  (public interval subtraction function)
  -  (internal interval processing)

## Notes and Other Information
- This is a static helper function, not exposed in the public API
- Both input intervals must be finite (assertion checks enforce this)
- Uses PostgreSQL's overflow-safe arithmetic functions to prevent integer overflow
- Raises ERRCODE_DATETIME_VALUE_OUT_OF_RANGE error if overflow occurs or result is infinite
- Part of PostgreSQL's timestamp/interval arithmetic implementation in src/backend/utils/adt/timestamp.c