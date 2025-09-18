# interval_justify_hours

## Location
src/backend/utils/adt/timestamp.c: 2960 - 3001

## Overview
Adjusts an interval so that the time component contains less than a whole day, converting excess time to days.

## Definition
```c
Datum interval_justify_hours(PG_FUNCTION_ARGS)
```

## Detailed Description
This function normalizes the time component of a PostgreSQL interval by ensuring it contains less than 24 hours worth of time. Any excess time (24 hours or more) is converted to whole days and added to the day field. This is particularly useful in scenarios where "1 day = 24 hours" equivalence is valid, such as interval subtraction and division operations.

The function preserves the month field unchanged and only adjusts the relationship between days and hours. It includes sign normalization logic to handle cases where the day and time components have different signs, ensuring consistent representation.

This is a more limited normalization compared to interval_justify_interval, focusing only on the hour-to-day conversion without affecting month calculations.

## Parameters / Member Variables
- `span`: Input interval to be hour-justified (from PG_GETARG_INTERVAL_P(0))
- `result`: Interval with normalized time and day fields
- `wholeday`: Temporary variable holding whole days extracted from excess time

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INTERVAL_P (PostgreSQL function call interface macro)
  - INTERVAL_NOT_FINITE (infinity checking macro for intervals)
  - TMODULO (time modulo operation macro)
  - USECS_PER_DAY (constant for microseconds per day conversion)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) (safe 32-bit addition with overflow detection)
  - PG_RETURN_INTERVAL_P (PostgreSQL return value macro)
- Called from (representative examples):
  - [timestamp_mi](../t/timestamp_mi.md) (called via DirectFunctionCall1 for hour justification)

## Notes and Other Information
- More limited than interval_justify_interval, only handling hour-to-day conversion
- Useful for operations where day and hour equivalence is important
- Called automatically by timestamp_mi to normalize timestamp subtraction results
- Handles infinite intervals by returning them unchanged
- Includes sign normalization between day and time fields
- Does not affect the month field, unlike interval_justify_interval
- Located at src/backend/utils/adt/timestamp.c:2960-3001