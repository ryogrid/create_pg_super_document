# date_mi_interval

## Location
[src/backend/utils/adt/date.c:1266-1282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1266-L1282)

## Overview
Subtracts an interval from a date value, returning a new timestamp result that represents the date minus the specified time interval.

## Definition
```c
Datum date_mi_interval(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements date arithmetic by subtracting an interval from a date value. It handles both positive and negative intervals, where subtracting a negative interval effectively performs addition.

The implementation strategy promotes the input date to a timestamp (without time zone) and then delegates the actual arithmetic to the existing `timestamp_mi_interval` function. This approach reuses the robust interval arithmetic logic already implemented for timestamps while providing the interface needed for date-interval subtraction operations.

The result is returned as a timestamp since subtracting an interval from a date may result in a value that includes time components beyond just the date portion.

## Parameters / Member Variables
- `PG_GETARG_DATEADT(0)`: The date value to subtract the interval from (`dateVal`)
- `PG_GETARG_INTERVAL_P(1)`: The interval to subtract from the date (`span`)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_DATEADT` - Extracts date argument
  - `PG_GETARG_INTERVAL_P` - Extracts interval argument
  - [date2timestamp](date2timestamp.md) - Converts date to timestamp
  - `DirectFunctionCall2` - Direct function call mechanism
  - [timestamp_mi_interval](../t/timestamp_mi_interval.md) - Timestamp interval subtraction function
  - `TimestampGetDatum`, `PointerGetDatum` - Datum conversion functions
  - `DateADT`, `Interval`, `Timestamp` - Data type definitions
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of PostgreSQL's date arithmetic operations infrastructure
- Supports both positive and negative intervals (subtraction and addition)
- Returns a timestamp rather than a date since intervals can include time components
- Located in src/backend/utils/adt/date.c:1266-1282
- Leverages existing timestamp interval arithmetic to ensure consistent behavior
- Companion function to `date_pl_interval` for complete date arithmetic support
- Used internally by PostgreSQL when processing SQL expressions like `date_value - interval_value`