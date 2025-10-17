# timestamptz_trunc_zone

## Location
[src/backend/utils/adt/timestamp.c:4988-5016](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L4988-L5016)

## Overview
Truncates a timestamptz (timestamp with timezone) value to specified units in a specified timezone, allowing timezone-aware truncation operations.

## Definition

```c
struct pg_itm tt,
			   *tm = &tt;
```
## Detailed Description
This function provides timezone-aware truncation of timestamp with timezone values. It takes three arguments: the units to truncate to, the timestamp value, and the timezone specification. The function first looks up the specified timezone and then delegates to `timestamptz_trunc_internal` to perform the actual truncation operation. The function handles infinite timestamp values by returning them unchanged, following the same pattern as `timestamptz_zone()`.

## Parameters / Member Variables
- `units` (text): The time units to truncate to (e.g., 'hour', 'day', 'month')
- `timestamp` (TimestampTz): The timestamp with timezone value to be truncated
- `zone` (text): The timezone specification for the truncation operation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (for extracting text arguments)
  - PG_GETARG_TIMESTAMPTZ (for extracting timestamptz argument)
  - TIMESTAMP_NOT_FINITE (macro for checking infinite timestamps)
  - PG_RETURN_TIMESTAMP (for returning infinite timestamps unchanged)
  - [lookup_timezone](../l/lookup_timezone.md) (for resolving timezone specification)
  - [timestamptz_trunc_internal](timestamptz_trunc_internal.md) (for performing the actual truncation)
  - PG_RETURN_TIMESTAMPTZ (for returning the result)
- Called from (representative examples):
  - No direct references found (likely called through SQL function interface)

## Notes and Other Information
- The function follows PostgreSQL's convention for handling infinite timestamp values by returning them unchanged
- Timezone lookup is performed for every call, which may have performance implications for high-frequency operations
- The actual truncation logic is delegated to `timestamptz_trunc_internal`, making this function primarily a wrapper that handles timezone resolution
- Located in src/backend/utils/adt/timestamp.c:4988-5016

## Simplified Source

```c
Datum timestamptz_trunc_zone(PG_FUNCTION_ARGS) {
    text *units = PG_GETARG_TEXT_PP(0);
    TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
    text *zone = PG_GETARG_TEXT_PP(2);
    TimestampTz result;
    pg_tz *tzp;

    // Return infinite timestamps as-is (following timestamptz_zone() pattern)
    if (TIMESTAMP_NOT_FINITE(timestamp))
        PG_RETURN_TIMESTAMP(timestamp);

    // Look up the specified timezone
    tzp = lookup_timezone(zone);

    // Delegate to internal function using specified timezone
    result = timestamptz_trunc_internal(units, timestamp, tzp);

    PG_RETURN_TIMESTAMPTZ(result);
}
```