# DetermineTimeZoneAbbrevOffsetInternal

## Location
src/backend/utils/adt/datetime.c: 1821 - 1863

## Overview
Internal workhorse function that attempts to resolve a timezone abbreviation to its GMT offset and DST status at a specific point in time using the IANA timezone database.

## Definition
```c
static bool DetermineTimeZoneAbbrevOffsetInternal(pg_time_t t, const char *abbr, pg_tz *tzp, int *offset, int *isdst)
```

## Detailed Description
This is the core implementation function that performs the actual lookup of timezone abbreviations in the timezone database. It serves as the workhorse for both `DetermineTimeZoneAbbrevOffset()` and `DetermineTimeZoneAbbrevOffsetTS()`.

The function performs the following steps:
1. Converts the input abbreviation to uppercase for consistent matching
2. Calls `pg_interpret_timezone_abbrev()` to look up the abbreviation's meaning at the specified time
3. If a match is found, converts the GMT offset sign to agree with `DetermineTimeZoneOffset()` convention and returns success
4. Returns false if no matching abbreviation is found in the timezone data

The function handles the case-insensitive nature of timezone abbreviations by normalizing to uppercase before lookup.

## Parameters / Member Variables
- `t`: pg_time_t representing the probe time for timezone abbreviation resolution
- `abbr`: The timezone abbreviation string to resolve
- `tzp`: Pointer to the timezone definition structure containing IANA timezone data
- `offset`: Output parameter receiving the GMT offset in seconds (with sign flipped to match convention)
- `isdst`: Output parameter receiving the DST status (0 for standard time, 1 for daylight time)

## Dependencies
- Functions called/Symbols referenced:
  - pg_time_t (type)
  - pg_tz (struct)
  - TZ_STRLEN_MAX (constant)
  - strlcpy
  - pg_toupper
  - pg_interpret_timezone_abbrev
- Called from (representative examples):
  - DetermineTimeZoneAbbrevOffset
  - DetermineTimeZoneAbbrevOffsetTS

## Notes and Other Information
- This is a static (internal) function not exposed outside the datetime.c module
- Performs case-insensitive abbreviation matching by converting input to uppercase
- The GMT offset sign is flipped to maintain consistency with other timezone offset functions
- Uses a fixed-size buffer (TZ_STRLEN_MAX + 1) for the uppercase abbreviation conversion
- Returns boolean success/failure rather than throwing errors, allowing callers to implement fallback strategies
- Located in src/backend/utils/adt/datetime.c:1821-1863