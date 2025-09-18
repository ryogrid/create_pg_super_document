# DecodeTimezoneAbbrev

## Location
[src/backend/utils/adt/datetime.c:3091-3147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L3091-L3147)

## Overview
DecodeTimezoneAbbrev interprets string tokens as timezone abbreviations and retrieves the corresponding timezone information, utilizing a cache for performance optimization.

## Definition
```c
int DecodeTimezoneAbbrev(int field, const char *lowtoken,
                        int *ftype, int *offset, pg_tz **tz,
                        DateTimeErrorExtra *extra)
```

## Detailed Description
This function performs timezone abbreviation lookup from PostgreSQL's timezone abbreviation table. It handles three types of timezone representations:

1. **TZ/DTZ**: Static timezone offsets with fixed UTC displacement
2. **DYNTZ**: Dynamic timezones that require full timezone object resolution
3. **UNKNOWN_FIELD**: Unrecognized abbreviations

The function implements a field-based cache mechanism to optimize repeated lookups of the same abbreviations. It uses binary search through the global timezone abbreviation table and handles both exact matches and truncated token matching.

## Parameters / Member Variables
- `field`: Cache index for this abbreviation lookup
- `lowtoken`: Lowercase timezone abbreviation string to lookup
- `ftype`: Output field type (TZ, DTZ, DYNTZ, or UNKNOWN_FIELD)
- `offset`: Output UTC offset in seconds (for TZ/DTZ types)
- `tz`: Output timezone object pointer (for DYNTZ types)
- `extra`: Additional error information structure

## Dependencies
- Functions called/Symbols referenced:
  - strncmp (standard C library)
  - [datebsearch](../d/datebsearch.md) (binary search function for date tokens)
  - [FetchDynamicTimeZone](../F/FetchDynamicTimeZone.md) (dynamic timezone resolver)
  - datetkn (date token structure type)
  - [pg_tz](../p/pg_tz.md) (PostgreSQL timezone type)
  - DateTimeErrorExtra (error information structure)
  - TOKMAXLEN (maximum token length constant)
  - UNKNOWN_FIELD, DYNTZ (field type constants)
  - DTERR_BAD_ZONE_ABBREV (error constant)

- Called from (representative examples):
  - [DecodeDateTime](DecodeDateTime.md) (main date/time parsing function)
  - [DecodeTimeOnly](DecodeTimeOnly.md) (time-only parsing function)
  - [DecodeTimezoneName](DecodeTimezoneName.md) (timezone name resolution function)

## Notes and Other Information
- Returns 0 on success, DTERR codes on failure
- Unknown abbreviations are not considered errors (return success with UNKNOWN_FIELD)
- Input string must be already lowercased
- Uses truncated token matching via strncmp with TOKMAXLEN
- Implements per-field caching using abbrevcache array for performance
- Dynamic timezones (DYNTZ) require additional resolution through FetchDynamicTimeZone
- Full timezone names like 'America/New_York' are not handled by this function
- Cache lookup occurs before table search for performance optimization