# timestamptz_pl_interval_at_zone

## Location
[src/backend/utils/adt/timestamp.c:3360-3370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L3360-L3370)

## Overview
Adds an interval to a timestamptz (timestamp with time zone) value, performing the calculation in a specified timezone rather than the session timezone.

## Definition
```c
Datum timestamptz_pl_interval_at_zone(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL built-in function that implements the addition operation between a timestamp with time zone and an interval, with the key difference that it allows specifying a particular timezone for the calculation. It serves as a wrapper around the internal function `timestamptz_pl_interval_internal`, handling the PostgreSQL function call interface by extracting arguments, resolving the timezone, and returning the result in the proper format. This is particularly useful for timezone-aware interval arithmetic where the calculation needs to be performed in a specific timezone context rather than the session's default timezone.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - Argument 0: `TimestampTz timestamp` - The timestamp with time zone to which the interval will be added
  - Argument 1: `Interval *span` - Pointer to the interval structure containing the time span to add
  - Argument 2: `text *zone` - Text representation of the timezone name in which to perform the calculation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMPTZ (macro for extracting timestamptz argument)
  - PG_GETARG_INTERVAL_P (macro for extracting interval pointer argument)
  - PG_GETARG_TEXT_PP (macro for extracting text argument)
  - [lookup_timezone](../l/lookup_timezone.md) (function to resolve timezone name to pg_tz structure)
  - [timestamptz_pl_interval_internal](timestamptz_pl_interval_internal.md) (internal implementation function)
  - PG_RETURN_TIMESTAMP (macro for returning timestamp result)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through specialized SQL functions)

## Notes and Other Information
- This function enables timezone-specific interval addition operations
- The timezone lookup is performed using `lookup_timezone` which converts the text timezone name to a `pg_tz` structure
- The actual computation logic is delegated to `timestamptz_pl_interval_internal` with the resolved timezone
- Located in src/backend/utils/adt/timestamp.c:3360-3370
- Returns a Datum containing the resulting timestamp with time zone
- Provides more control over timezone handling compared to the basic `timestamptz_pl_interval` function

## Simplified Source

```c
Datum timestamptz_pl_interval_at_zone(PG_FUNCTION_ARGS) {
    TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
    Interval *span = PG_GETARG_INTERVAL_P(1);
    text *zone_text = PG_GETARG_TEXT_PP(2);

    // Resolve timezone name to pg_tz structure
    pg_tz *attimezone = lookup_timezone(zone_text);

    // Delegate to internal function with specified timezone
    PG_RETURN_TIMESTAMP(timestamptz_pl_interval_internal(timestamp, span, attimezone));
}
```