# pg_tz

## Location
src/timezone/pgtz.h: 65 - 81

## Overview
The pg_tz struct represents a PostgreSQL timezone object, containing the canonical timezone name and complete timezone state information.

## Definition
```c
struct pg_tz
{
    /* TZname contains the canonically-cased name of the timezone */
    char        TZname[TZ_STRLEN_MAX + 1];
    struct state state;
};
```

## Detailed Description
The pg_tz struct is PostgreSQL's primary timezone object that encapsulates both the canonical name of a timezone and all of its computational state. This structure serves as the interface between PostgreSQL's high-level timezone operations and the underlying timezone calculation engine. It maintains the official timezone name (with canonical casing) and embeds a complete state structure containing all the transition rules, leap second data, and type information needed for accurate timezone conversions.

## Parameters / Member Variables
- `TZname`: Null-terminated string containing the canonically-cased timezone name (e.g., "America/New_York")
- `state`: Complete timezone state structure containing all transition and calculation data

## Dependencies
- Functions called/Symbols referenced:
  - struct state (timezone state information)
  - TZ_STRLEN_MAX (maximum timezone name length constant)
- Called from (representative examples):
  - check_timezone (timezone validation functions)
  - assign_timezone (timezone assignment functions)
  - timetz_zone (time with timezone operations)
  - GetCurrentTimeUsec (current time functions)
  - DecodeDateTime (datetime parsing functions)
  - DetermineTimeZoneOffset (timezone offset calculations)
  - timestamp2tm (timestamp conversion functions)
  - pg_localtime (local time functions)
  - pg_next_dst_boundary (DST boundary calculations)
  - pg_interpret_timezone_abbrev (abbreviation interpretation)
  - pg_get_timezone_offset (timezone offset queries)
  - pg_get_timezone_name (timezone name queries)
  - pg_tzset (timezone setting functions)

## Notes and Other Information
This structure is the primary interface for timezone operations throughout PostgreSQL. The canonical name ensures consistent timezone identification regardless of how the timezone was originally specified. The embedded state structure contains all the computational data loaded from timezone database files. This design allows PostgreSQL to efficiently cache timezone information and perform fast timezone conversions without repeatedly accessing timezone files. The structure is widely used across datetime, timestamp, and timezone-related operations throughout the PostgreSQL codebase.