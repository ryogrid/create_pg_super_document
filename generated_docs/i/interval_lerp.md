# interval_lerp

## Location
src/backend/utils/adt/orderedsetaggs.c: 512 - 525

## Overview
Performs linear interpolation between two interval values, used in percentile calculations for ordered-set aggregate functions that operate on interval data types.

## Definition
```c
static Datum interval_lerp(Datum lo, Datum hi, double pct)
```

## Detailed Description
This static function implements linear interpolation (lerp) for PostgreSQL interval values. Unlike the simpler arithmetic interpolation used for numeric types, interval interpolation requires using PostgreSQL's interval arithmetic functions to handle the complex structure of interval data (years, months, days, hours, minutes, seconds, microseconds). The function computes the interpolated interval value that is `pct` fraction of the way from the lower bound interval to the upper bound interval.

The interpolation is performed using the formula: `result = lo + pct * (hi - lo)`, implemented through interval arithmetic operations: subtraction (interval_mi), multiplication (interval_mul), and addition (interval_pl).

## Parameters / Member Variables
- `lo`: Lower bound interval value as a Datum
- `hi`: Upper bound interval value as a Datum
- `pct`: Percentage/fraction (0.0 to 1.0) indicating the interpolation point

## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall2 (calls PostgreSQL functions with two arguments)
  - [interval_mi](interval_mi.md) (interval subtraction function)
  - [interval_mul](interval_mul.md) (interval multiplication function)
  - [interval_pl](interval_pl.md) (interval addition function)
  - Float8GetDatumFast (fast conversion of double to Datum)
- Called from (representative examples):
  - [percentile_cont_interval_final](../p/percentile_cont_interval_final.md)
  - [percentile_cont_interval_multi_final](../p/percentile_cont_interval_multi_final.md)

## Notes and Other Information
- This is a static helper function used internally within the ordered-set aggregates module
- Uses PostgreSQL's function call interface (DirectFunctionCall2) to leverage existing interval arithmetic operations
- Part of PostgreSQL's implementation of the PERCENTILE_CONT aggregate function for interval data types
- The complexity compared to float8_lerp reflects the sophisticated internal structure of PostgreSQL interval types
- Handles all components of intervals (years, months, days, time) through the underlying interval arithmetic functions