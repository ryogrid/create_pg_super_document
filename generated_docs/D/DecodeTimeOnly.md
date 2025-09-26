# DecodeTimeOnly

## Location
[src/backend/utils/adt/datetime.c:1864-2397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L1864-L2397)

## Overview
Interprets parsed string fields as time-only values with optional timezone support, handling various time formats including ISO time, Julian dates, timezone abbreviations, and special keywords.

## Definition
```c
int DecodeTimeOnly(char **field, int *ftype, int nf, int *dtype, struct pg_tm *tm, fsec_t *fsec, int *tzp, DateTimeErrorExtra *extra)
```

## Detailed Description
This function processes an array of parsed time-related string fields and interprets them as time values, with optional timezone information. It's designed specifically for SQL TIME and TIME WITH TIME ZONE types. The function handles numerous time formats and representations:

- Standard time formats (HH:MM:SS, HH:MM)
- ISO time format following 't' prefix
- Julian date representations with optional fractional time
- Timezone abbreviations and full timezone names
- Special keywords like 'now', 'zulu'
- AM/PM modifiers
- Daylight saving time modifiers

The function performs extensive validation and field mask checking to ensure consistent time representation. It handles timezone resolution through multiple methods:
1. Named timezones (e.g., 'America/New_York')
2. Dynamic timezone abbreviations that require date context
3. Static timezone offsets
4. Session timezone as fallback

## Parameters / Member Variables
- `field`: Array of parsed string fields to interpret
- `ftype`: Array indicating the type of each field (DTK_DATE, DTK_TIME, DTK_NUMBER, etc.)
- `nf`: Number of fields in the arrays
- `dtype`: Output parameter set to DTK_TIME or DTK_DATE
- `tm`: Output pg_tm structure containing the decoded time components
- `fsec`: Output fractional seconds component
- `tzp`: Output timezone offset in seconds (NULL if timezone not supported)
- `extra`: Structure for additional error information

## Dependencies
- Functions called/Symbols referenced:
  - [DecodeDate](DecodeDate.md)
  - [DecodeTime](DecodeTime.md)
  - [DecodeTimezone](DecodeTimezone.md)
  - [DecodeNumberField](DecodeNumberField.md)
  - [DecodeNumber](DecodeNumber.md)
  - [DecodeSpecial](DecodeSpecial.md)
  - [DecodeTimezoneAbbrev](DecodeTimezoneAbbrev.md)
  - [DetermineTimeZoneOffset](DetermineTimeZoneOffset.md)
  - [DetermineTimeZoneAbbrevOffset](DetermineTimeZoneAbbrevOffset.md)
  - [ValidateDate](../V/ValidateDate.md)
  - [pg_tzset](../p/pg_tzset.md)
  - [pg_get_timezone_offset](../p/pg_get_timezone_offset.md)
  - [GetCurrentTimeUsec](../G/GetCurrentTimeUsec.md)
  - [GetCurrentDateTime](../G/GetCurrentDateTime.md)
  - [j2date](../j/j2date.md) (Julian to date conversion)
  - [dt2time](../d/dt2time.md) (day time to time components)
  - [time_overflows](../t/time_overflows.md)
- Called from (representative examples):
  - [time_in](../t/time_in.md)
  - [timetz_in](../t/timetz_in.md)

## Notes and Other Information
- Designed specifically for SQL TIME WITH TIME ZONE support, with notable limitations due to SQL standard ambiguities
- Supports both standard and daylight time abbreviations, forcing appropriate GMT offset usage
- Handles Julian date format with optional fractional time components
- Performs extensive field mask validation to prevent duplicate or conflicting time components
- Automatically applies session timezone when no explicit timezone is specified
- The function contains special logic for resolving dynamic timezone abbreviations that require date context
- Returns DTERR error codes for various parsing and validation failures
- Supports both 12-hour (AM/PM) and 24-hour time formats
- Located in src/backend/utils/adt/datetime.c:1864-2397