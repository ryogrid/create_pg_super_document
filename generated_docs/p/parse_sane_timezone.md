# parse_sane_timezone

## Location
src/backend/utils/adt/timestamp.c: 489 - 557

## Overview
Parses a timezone specification string and returns its timezone offset value, with comprehensive validation and error handling.

## Definition
```c
static int parse_sane_timezone(struct pg_tm *tm, text *zone)
```

## Detailed Description
The `parse_sane_timezone` function attempts to parse and validate a timezone specification, returning the corresponding timezone offset in seconds. It handles multiple timezone formats including numeric offsets, timezone abbreviations, and full timezone names. The function performs strict validation to prevent ambiguous or invalid input, particularly rejecting numeric timezone strings that start with digits without proper sign prefixes. It uses a multi-stage approach: first attempting numeric parsing via `DecodeTimezone`, then falling back to timezone name resolution through `DecodeTimezoneName` and appropriate offset determination functions.

## Parameters / Member Variables
- `tm` (struct pg_tm *): Time structure used for context in dynamic timezone resolution
- `zone` (text *): PostgreSQL text object containing the timezone specification string

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring_buffer
  - isdigit
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errhint](../e/errhint.md)
  - [DecodeTimezone](../D/DecodeTimezone.md)
  - [DecodeTimezoneName](../D/DecodeTimezoneName.md)
  - [DetermineTimeZoneAbbrevOffset](../D/DetermineTimeZoneAbbrevOffset.md)
  - DetermineTimeZoneOffset
- Types referenced:
  - [pg_tm](pg_tm.md)
  - [pg_tz](pg_tz.md)
- Constants referenced:
  - TZ_STRLEN_MAX
  - ERRCODE_INVALID_PARAMETER_VALUE
  - DTERR_TZDISP_OVERFLOW
  - DTERR_BAD_FORMAT
  - TZNAME_FIXED_OFFSET
  - TZNAME_DYNTZ
- Called from (representative examples):
  - [make_timestamptz_at_timezone](../m/make_timestamptz_at_timezone.md)

## Notes and Other Information
- Static function, only accessible within timestamp.c
- Rejects numeric timezone strings starting with digits without "+"/"-" prefix for security
- Handles three types of timezone specifications: numeric offsets, fixed abbreviations, dynamic abbreviations, and full zone names
- Uses context time (tm parameter) for resolving dynamic timezone abbreviations that change with DST
- Located in src/backend/utils/adt/timestamp.c:489-557
- The tm_isdst field may be updated inconsistently across different code paths (noted in comments)
- Provides detailed error messages distinguishing between format errors and overflow conditions
- Returns timezone offset in seconds (negative values indicate west of UTC)