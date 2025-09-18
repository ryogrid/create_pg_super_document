# in_range_timestamptz_interval

## Location
[src/backend/utils/adt/timestamp.c:3798-3834](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L3798-L3834)

## Overview
Implements SQL window function RANGE BETWEEN support for timestamptz values with interval offsets, determining if a value falls within a specified range from a base timestamp.

## Definition
```c
Datum in_range_timestamptz_interval(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's window function infrastructure, specifically implementing the RANGE BETWEEN clause for timestamptz data types with interval offsets. It determines whether a given timestamp with time zone (val) falls within a range defined by a base timestamptz and an interval offset. The function follows SQL specification requirements for range-based window functions.

The function handles special cases involving infinite timestamps and intervals, ensuring consistent behavior across boundary conditions. It validates that the offset interval is not negative (per SQL spec) and performs appropriate arithmetic operations (addition or subtraction) based on the sub parameter.

## Parameters / Member Variables
- `val`: The timestamptz value to test for inclusion in the range
- `base`: The base timestamptz value from which the range is calculated
- `offset`: The interval offset defining the range size (must be non-negative)
- `sub`: Boolean indicating whether to subtract (true) or add (false) the offset from base
- `less`: Boolean indicating whether to test for less-than-or-equal (true) or greater-than-or-equal (false) comparison

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMPTZ
  - PG_GETARG_INTERVAL_P
  - PG_GETARG_BOOL
  - [interval_sign](interval_sign.md)
  - INTERVAL_IS_NOEND
  - TIMESTAMP_IS_NOBEGIN
  - TIMESTAMP_IS_NOEND
  - [timestamptz_mi_interval_internal](../t/timestamptz_mi_interval_internal.md)
  - [timestamptz_pl_interval_internal](../t/timestamptz_pl_interval_internal.md)
- Called from:
  - No direct references found (likely called via function catalog)

## Notes and Other Information
- Part of SQL window function RANGE BETWEEN support
- Enforces SQL specification requirement that offset intervals must not be negative
- Handles infinite timestamp and interval edge cases gracefully
- Does not currently implement overflow hazard avoidance
- Returns boolean result indicating whether val is within the computed range