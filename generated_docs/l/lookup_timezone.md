# lookup_timezone

## Location
src/backend/utils/adt/timestamp.c: 558 - 571

## Overview
Looks up a timezone by name and returns a pg_tz structure, serving as a text-based wrapper around DecodeTimezoneNameToTz.

## Definition
```c
static pg_tz *lookup_timezone(text *zone)
```

## Detailed Description
The `lookup_timezone` function is a utility function that converts a PostgreSQL text datum containing a timezone name into a pg_tz structure. It acts as a simple wrapper around `DecodeTimezoneNameToTz`, handling the conversion from PostgreSQL's text type to a C string before performing the timezone lookup. This function is used internally when timezone operations need to work with text arguments representing timezone names.

## Parameters / Member Variables
- `zone` (text *): PostgreSQL text object containing the timezone name to look up

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring_buffer
  - [DecodeTimezoneNameToTz](../D/DecodeTimezoneNameToTz.md)
- Constants referenced:
  - TZ_STRLEN_MAX
- Types referenced:
  - [pg_tz](../p/pg_tz.md)
- Called from (representative examples):
  - [timestamptz_pl_interval_at_zone](../t/timestamptz_pl_interval_at_zone.md)
  - [timestamptz_mi_interval_at_zone](../t/timestamptz_mi_interval_at_zone.md)
  - [timestamptz_trunc_zone](../t/timestamptz_trunc_zone.md)
  - [generate_series_timestamptz_internal](../g/generate_series_timestamptz_internal.md)

## Notes and Other Information
- Static function, only accessible within timestamp.c
- Simple wrapper function that primarily handles text-to-string conversion
- Uses a fixed-size buffer (TZ_STRLEN_MAX + 1) for timezone name conversion
- Located in src/backend/utils/adt/timestamp.c:558-571
- Returns NULL if the timezone name is not found or invalid (inherited from DecodeTimezoneNameToTz behavior)
- Used in timezone-aware arithmetic and truncation operations
- Functionally equivalent to DecodeTimezoneNameToTz but accepts text input instead of C string