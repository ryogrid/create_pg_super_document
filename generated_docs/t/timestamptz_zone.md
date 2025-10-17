# timestamptz_zone

## Location
[src/backend/utils/adt/timestamp.c:6402-6465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L6402-L6465)

## Overview
This function evaluates a timestamp with time zone at a specified time zone and returns the corresponding timestamp without time zone, effectively converting a timestamptz to local time in the target timezone.

## Definition

```c
struct pg_tm tm;
```
## Detailed Description
The  function converts a timestamp with time zone to a plain timestamp by interpreting the timestamptz value in a specified target timezone rather than the session's current timezone. This function accepts two arguments: a timezone specification (as text) and a timestamptz value.

The function handles three different types of timezone specifications:

1. **Fixed-offset abbreviations** (like '+05:00', 'PST-8'): The function applies the fixed offset directly using .

2. **Dynamic-offset abbreviations** (like 'PST', 'EDT'): These abbreviations can have different offsets depending on daylight saving time rules. The function uses  to resolve the actual offset at the given timestamp.

3. **Full zone names** (like 'America/New_York', 'Europe/London'): For complete timezone names, the function performs a full timezone conversion by decomposing the timestamptz with the target timezone using , then reconstructing it as a plain timestamp.

The function includes comprehensive error handling for out-of-range values and invalid timestamps, and properly handles non-finite timestamp values by passing them through unchanged.

## Parameters / Member Variables
- Argument 0:  (text) - The target timezone specification as a text string
- Argument 1:  (TimestampTz) - The input timestamp with timezone value to convert

## Dependencies
- Functions called/Symbols referenced:
  -  - retrieves the timezone text argument
  -  - retrieves the timestamptz argument
  -  - checks for infinite timestamp values
  -  - converts text to C string
  -  - parses and categorizes timezone names
  -  - applies timezone offset to convert timestamptz to local time
  -  - resolves dynamic timezone abbreviations
  -  - decomposes timestamp with timezone consideration
  -  - reconstructs timestamp from components
  -  - validates the resulting timestamp
  -  - returns the converted timestamp result
- Called from:
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- This function implements the PostgreSQL SQL function for timezone conversion (likely accessible via AT TIME ZONE syntax)
- The function supports all PostgreSQL timezone specification formats including abbreviations, offsets, and full zone names
- Located in  at lines 6402-6465
- Handles daylight saving time transitions correctly for dynamic timezone abbreviations
- Uses a maximum timezone name length of  characters
- The function follows PostgreSQL's V1 calling convention for SQL functions
- Comprehensive error reporting ensures that invalid timezone names or out-of-range timestamps are properly handled
- The result is always a plain timestamp (without timezone information) representing the local time in the specified timezone

## Simplified Source

```c
Datum timestamptz_zone(PG_FUNCTION_ARGS) {
    text *zone = PG_GETARG_TEXT_PP(0);
    TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
    Timestamp result;
    int tz, type, val;
    char tzname[TZ_STRLEN_MAX + 1];
    pg_tz *tzp;

    // Handle infinite timestamps
    if (TIMESTAMP_NOT_FINITE(timestamp))
        PG_RETURN_TIMESTAMP(timestamp);

    // Parse timezone specification
    text_to_cstring_buffer(zone, tzname, sizeof(tzname));
    type = DecodeTimezoneName(tzname, &val, &tzp);

    if (type == TZNAME_FIXED_OFFSET) {
        // Fixed offset like '+05:00' - negate for reverse conversion
        tz = -val;
        result = dt2local(timestamp, tz);
    }
    else if (type == TZNAME_DYNTZ) {
        // Dynamic abbreviation - resolve offset at this timestamp
        int isdst;
        tz = DetermineTimeZoneAbbrevOffsetTS(timestamp, tzname, tzp, &isdst);
        result = dt2local(timestamp, tz);
    }
    else {
        // Full zone name - decompose and reconstruct without timezone
        struct pg_tm tm;
        fsec_t fsec;

        if (timestamp2tm(timestamp, &tz, &tm, &fsec, NULL, tzp) != 0)
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                    errmsg("timestamp out of range")));
        if (tm2timestamp(&tm, fsec, NULL, &result) != 0)
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                    errmsg("timestamp out of range")));
    }

    // Validate result
    if (!IS_VALID_TIMESTAMP(result))
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                errmsg("timestamp out of range")));

    PG_RETURN_TIMESTAMP(result);
}
```