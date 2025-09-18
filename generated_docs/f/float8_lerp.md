# float8_lerp

## Location
src/backend/utils/adt/orderedsetaggs.c: 503 - 511

## Overview
Performs linear interpolation between two float8 (double precision) values, used in percentile calculations for ordered-set aggregate functions.

## Definition
```c
static Datum float8_lerp(Datum lo, Datum hi, double pct)
```

## Detailed Description
This static function implements linear interpolation (lerp) for double precision floating point values. It takes two Datum values representing the lower and upper bounds, along with a percentage value, and computes the interpolated value at that percentage point between the bounds. The function is specifically designed to support percentile calculations in PostgreSQL's ordered-set aggregate functions by providing smooth transitions between discrete data points.

The interpolation formula used is: `result = lo + pct * (hi - lo)`, which gives the value that is `pct` fraction of the way from `lo` to `hi`.

## Parameters / Member Variables
- `lo`: Lower bound value as a Datum (converted to double precision)
- `hi`: Upper bound value as a Datum (converted to double precision) 
- `pct`: Percentage/fraction (0.0 to 1.0) indicating the interpolation point

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetFloat8](../D/DatumGetFloat8.md) (converts Datum to double)
  - [Float8GetDatum](../F/Float8GetDatum.md) (converts double to Datum)
- Called from (representative examples):
  - [percentile_cont_float8_final](../p/percentile_cont_float8_final.md)
  - [percentile_cont_float8_multi_final](../p/percentile_cont_float8_multi_final.md)

## Notes and Other Information
- This is a static helper function used internally within the ordered-set aggregates module
- The function assumes input values are valid float8 Datums and that pct is between 0.0 and 1.0
- Part of PostgreSQL's implementation of the PERCENTILE_CONT aggregate function for numeric data types
- Linear interpolation provides continuous results even when the exact percentile falls between discrete values in the sorted dataset