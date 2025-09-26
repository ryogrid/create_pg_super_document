# pg_tz

## Location
[src/timezone/pgtz.h:65-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/pgtz.h#L65-L81)

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
  - [check_timezone](../c/check_timezone.md) (timezone validation functions)
  - [assign_timezone](../a/assign_timezone.md) (timezone assignment functions)
  - [timetz_zone](../t/timetz_zone.md) (time with timezone operations)
  - [GetCurrentTimeUsec](../G/GetCurrentTimeUsec.md) (current time functions)
  - [DecodeDateTime](../D/DecodeDateTime.md) (datetime parsing functions)
  - [DetermineTimeZoneOffset](../D/DetermineTimeZoneOffset.md) (timezone offset calculations)
  - [timestamp2tm](../t/timestamp2tm.md) (timestamp conversion functions)
  - [pg_localtime](pg_localtime.md) (local time functions)
  - [pg_next_dst_boundary](pg_next_dst_boundary.md) (DST boundary calculations)
  - [pg_interpret_timezone_abbrev](pg_interpret_timezone_abbrev.md) (abbreviation interpretation)
  - [pg_get_timezone_offset](pg_get_timezone_offset.md) (timezone offset queries)
  - [pg_get_timezone_name](pg_get_timezone_name.md) (timezone name queries)
  - [pg_tzset](pg_tzset.md) (timezone setting functions)

## Notes and Other Information
This structure is the primary interface for timezone operations throughout PostgreSQL. The canonical name ensures consistent timezone identification regardless of how the timezone was originally specified. The embedded state structure contains all the computational data loaded from timezone database files. This design allows PostgreSQL to efficiently cache timezone information and perform fast timezone conversions without repeatedly accessing timezone files. The structure is widely used across datetime, timestamp, and timezone-related operations throughout the PostgreSQL codebase.