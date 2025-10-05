# date_pl_interval

## Location
[src/backend/utils/adt/date.c:1246-1265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1246-L1265)

## Overview
Adds an interval to a date value, returning a new timestamp result that represents the date plus the specified time interval.

## Definition
```c
Datum date_pl_interval(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements date arithmetic by adding an interval to a date value. It handles both positive and negative intervals, effectively supporting both addition and subtraction operations through the interval's sign.

The implementation strategy promotes the input date to a timestamp (without time zone) and then delegates the actual arithmetic to the existing `timestamp_pl_interval` function. This approach reuses the robust interval arithmetic logic already implemented for timestamps while providing the interface needed for date-interval operations.

The result is returned as a timestamp since adding an interval to a date may result in a value that includes time components beyond just the date portion.

## Parameters / Member Variables
- `PG_GETARG_DATEADT(0)`: The date value to add the interval to (`dateVal`)
- `PG_GETARG_INTERVAL_P(1)`: The interval to add to the date (`span`)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_DATEADT` - Extracts date argument
  - `PG_GETARG_INTERVAL_P` - Extracts interval argument
  - [date2timestamp](date2timestamp.md) - Converts date to timestamp
  - `DirectFunctionCall2` - Direct function call mechanism
  - [timestamp_pl_interval](../t/timestamp_pl_interval.md) - Timestamp interval addition function
  - `[TimestampGetDatum](../T/TimestampGetDatum.md)`, `PointerGetDatum` - Datum conversion functions
  - `DateADT`, `Interval`, `Timestamp` - Data type definitions
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of PostgreSQL's date arithmetic operations infrastructure
- Supports both positive and negative intervals (addition and subtraction)
- Returns a timestamp rather than a date since intervals can include time components
- Located in src/backend/utils/adt/date.c:1246-1265
- Leverages existing timestamp interval arithmetic to ensure consistent behavior
- Used internally by PostgreSQL when processing SQL expressions like `date_value + interval_value`

## Simplified Source

```c
Datum
date_pl_interval(PG_FUNCTION_ARGS)
{
    // Extract date and interval arguments
    DateADT dateVal = PG_GETARG_DATEADT(0);
    Interval *span = PG_GETARG_INTERVAL_P(1);

    // Convert date to timestamp and delegate to timestamp interval addition
    Timestamp dateStamp = date2timestamp(dateVal);

    return DirectFunctionCall2(timestamp_pl_interval,
                              TimestampGetDatum(dateStamp),
                              PointerGetDatum(span));
}
```