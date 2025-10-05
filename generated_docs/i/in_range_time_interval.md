# in_range_time_interval

## Location
[src/backend/utils/adt/date.c:2098-2139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2098-L2139)

## Overview
The  function provides window function support for determining if a time value falls within a specified range relative to a base time and an interval offset.

## Definition

```c
struct pg_tm tt,
				   *tm = &tt;
```
## Detailed Description
This function is a support function for PostgreSQL's window function RANGE clause when working with time data types. It determines whether a given time value falls within a range defined by a base time plus or minus an interval offset. Unlike the regular time arithmetic functions, this function does not perform day wraparound behavior, which is important for window function semantics. The function handles potential integer overflow when adding large intervals and validates that the offset interval is not negative.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - The time value to test for inclusion in the range
  - Argument 1:  - The base time value that defines the range center
  - Argument 2:  - Pointer to the interval that defines the range size
  - Argument 3:  - Whether to subtract the offset from base (true) or add it (false)
  - Argument 4:  - Whether to perform less-than-or-equal (true) or greater-than-or-equal (false) comparison

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMEADT
  - PG_GETARG_INTERVAL_P
  - PG_GETARG_BOOL
  - PG_RETURN_BOOL
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md) (for overflow detection)
  - ereport (for error handling)
- Data types used:
  - TimeADT
  - Interval
  - [bool](../b/bool.md)
- Called from (representative examples):
  - No direct references found (likely called via SQL window function system)

## Notes and Other Information
- This function is specifically designed for window functions and does not perform the day wraparound behavior of /
- Negative intervals are rejected with error code 
- The function only uses the  field of the interval, disregarding month and day components
- Overflow detection is performed when adding intervals to prevent integer overflow
- Subtraction operations cannot overflow in this context
- The function returns a boolean result indicating whether the value is within the specified range
- Located in src/backend/utils/adt/date.c:2098-2139

## Simplified Source

```c
Datum
in_range_time_interval(PG_FUNCTION_ARGS)
{
    // Extract function arguments
    TimeADT val = PG_GETARG_TIMEADT(0);     // Value to test
    TimeADT base = PG_GETARG_TIMEADT(1);    // Base time
    Interval *offset = PG_GETARG_INTERVAL_P(2);  // Range offset
    bool sub = PG_GETARG_BOOL(3);           // Subtract offset?
    bool less = PG_GETARG_BOOL(4);          // Less-than comparison?

    // Reject negative intervals (including -infinity)
    if (offset->time < 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
                 errmsg("invalid preceding or following size in window function")));

    // Calculate range boundary (no wraparound behavior like regular time arithmetic)
    TimeADT sum;
    if (sub)
        sum = base - offset->time;  // Subtraction cannot overflow
    else if (pg_add_s64_overflow(base, offset->time, &sum))
        PG_RETURN_BOOL(less);       // Overflow means out of range

    // Check if value is within range
    if (less)
        PG_RETURN_BOOL(val <= sum);
    else
        PG_RETURN_BOOL(val >= sum);
}
``` 