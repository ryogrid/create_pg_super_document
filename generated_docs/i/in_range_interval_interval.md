# in_range_interval_interval

## Location
[src/backend/utils/adt/timestamp.c:3876-3925](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L3876-L3925)

## Overview
Implements SQL window function RANGE BETWEEN support for interval values with interval offsets, determining if an interval value falls within a specified range from a base interval.

## Definition
```c
Datum in_range_interval_interval(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides RANGE BETWEEN clause support for interval data types with interval offsets in PostgreSQL window functions. It determines whether a given interval (val) falls within a range defined by a base interval and an interval offset. This function operates entirely within the interval domain, performing interval arithmetic and comparisons.

The function validates that the offset interval is non-negative per SQL specification requirements and handles special cases involving infinite intervals. It uses DirectFunctionCall2 to invoke interval arithmetic operations and interval_cmp_internal for comparisons, ensuring consistent behavior with PostgreSQL's interval comparison semantics.

## Parameters / Member Variables
- `val`: The interval value to test for inclusion in the range
- `base`: The base interval value from which the range is calculated
- `offset`: The interval offset defining the range size (must be non-negative)
- `sub`: Boolean indicating whether to subtract (true) or add (false) the offset from base
- `less`: Boolean indicating whether to test for less-than-or-equal (true) or greater-than-or-equal (false) comparison

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INTERVAL_P
  - PG_GETARG_BOOL
  - [interval_sign](interval_sign.md)
  - INTERVAL_IS_NOEND
  - INTERVAL_IS_NOBEGIN
  - [DatumGetIntervalP](../D/DatumGetIntervalP.md)
  - DirectFunctionCall2
  - [interval_mi](interval_mi.md)
  - [interval_pl](interval_pl.md)
  - [IntervalPGetDatum](../I/IntervalPGetDatum.md)
  - [interval_cmp_internal](interval_cmp_internal.md)
- Called from:
  - No direct references found (likely called via function catalog)

## Notes and Other Information
- Part of SQL window function RANGE BETWEEN support for interval data types
- Enforces SQL specification requirement that offset intervals must not be negative
- Uses interval_cmp_internal for consistent comparison semantics
- Handles infinite interval edge cases with INTERVAL_IS_NOEND and INTERVAL_IS_NOBEGIN macros
- Does not currently implement overflow hazard avoidance
- Performs all operations within the interval domain, maintaining interval semantics throughout

## Simplified Source
```c
Datum in_range_interval_interval(PG_FUNCTION_ARGS) {
    Interval *val = PG_GETARG_INTERVAL_P(0);
    Interval *base = PG_GETARG_INTERVAL_P(1);
    Interval *offset = PG_GETARG_INTERVAL_P(2);
    bool sub = PG_GETARG_BOOL(3);
    bool less = PG_GETARG_BOOL(4);

    // SQL spec requires non-negative offset
    if (interval_sign(offset) < 0)
        ereport(ERROR, "invalid window function offset");

    // Handle infinite interval edge cases
    if (INTERVAL_IS_NOEND(offset) &&
        (sub ? INTERVAL_IS_NOEND(base) : INTERVAL_IS_NOBEGIN(base)))
        PG_RETURN_BOOL(true);

    // Calculate base +/- offset
    Interval *sum;
    if (sub)
        sum = DatumGetIntervalP(DirectFunctionCall2(interval_mi,
                                                    IntervalPGetDatum(base),
                                                    IntervalPGetDatum(offset)));
    else
        sum = DatumGetIntervalP(DirectFunctionCall2(interval_pl,
                                                    IntervalPGetDatum(base),
                                                    IntervalPGetDatum(offset)));

    // Return comparison result using interval comparison
    if (less)
        PG_RETURN_BOOL(interval_cmp_internal(val, sum) <= 0);
    else
        PG_RETURN_BOOL(interval_cmp_internal(val, sum) >= 0);
}
```