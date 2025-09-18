# DecodeTimezoneNameToTz

## Location
[src/backend/utils/adt/datetime.c:3245-3272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L3245-L3272)

## Overview
Interprets a string as a timezone abbreviation or name and returns a pg_tz pointer, throwing an error if the name is not recognized.

## Definition
```c
pg_tz *DecodeTimezoneNameToTz(const char *tzname)
```

## Detailed Description
This function serves as a simple wrapper around DecodeTimezoneName that ensures a pg_tz pointer is always returned. It handles both timezone abbreviations and full timezone names. When the input represents a fixed-offset abbreviation (like "+05:00"), it creates a pg_tz descriptor for that specific offset. For named timezones, it returns the corresponding pg_tz structure. The function throws an error if the timezone name is not recognized.

## Parameters / Member Variables
- `tzname`: A null-terminated string containing the timezone name or abbreviation to decode

## Dependencies
- Functions called/Symbols referenced:
  - [DecodeTimezoneName](DecodeTimezoneName.md)
  - [pg_tzset_offset](../p/pg_tzset_offset.md)
  - TZNAME_FIXED_OFFSET
  - [pg_tz](../p/pg_tz.md)
- Called from (representative examples):
  - [lookup_timezone](../l/lookup_timezone.md)

## Notes and Other Information
- This function flips the sign convention when creating fixed-offset timezones to conform to POSIX standards
- It provides a unified interface for timezone processing that always returns a pg_tz pointer
- The function is declared in src/include/utils/datetime.h with TZNAME_ZONE constant
- Error handling is delegated to the underlying DecodeTimezoneName function