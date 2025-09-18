# DecodeTimezoneAbbrevPrefix

## Location
[src/backend/utils/adt/datetime.c:3273-3339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L3273-L3339)

## Overview
Interprets a prefix of a string as a timezone abbreviation, matching the longest possible prefix and applying downcasing internally for formatting operations.

## Definition
```c
int DecodeTimezoneAbbrevPrefix(const char *str, int *offset, pg_tz **tz)
```

## Detailed Description
This function provides timezone abbreviation parsing functionality specifically adapted for formatting operations. Unlike DecodeTimezoneAbbrev(), it matches the longest possible prefix of the input string rather than requiring a complete match, and handles downcasing internally. The function searches through successively truncated strings to find the longest valid timezone abbreviation prefix. For fixed-offset abbreviations, it returns the GMT offset; for dynamic abbreviations, it returns the corresponding pg_tz structure.

## Parameters / Member Variables
- `str`: Input string containing potential timezone abbreviation prefix
- `offset`: Output parameter set to GMT offset for fixed-offset abbreviations
- `tz`: Output parameter set to pg_tz structure for dynamic abbreviations

## Dependencies
- Functions called/Symbols referenced:
  - [datebsearch](../d/datebsearch.md)
  - [pg_tolower](../p/pg_tolower.md)
  - [FetchDynamicTimeZone](../F/FetchDynamicTimeZone.md)
  - datetkn
  - TOKMAXLEN
  - DYNTZ
  - DateTimeErrorExtra
  - [pg_tz](../p/pg_tz.md)
- Called from (representative examples):
  - [DCH_from_char](DCH_from_char.md)

## Notes and Other Information
- Returns the length of the matched timezone abbreviation, or -1 if no match found
- Uses binary search optimization through datebsearch for efficient lookup
- Handles both fixed-offset (numeric) and dynamic (named) timezone abbreviations
- The function requires zoneabbrevtbl to be initialized, returning -1 immediately if not available
- Downcasing is performed internally using pg_tolower for case-insensitive matching
- Declared in src/include/utils/datetime.h with TZNAME_ZONE constant