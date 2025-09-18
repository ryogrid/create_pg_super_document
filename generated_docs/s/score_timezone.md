# score_timezone

## Location
src/bin/initdb/findtimezone.c: 234 - 319

## Overview
Evaluates how well a specific timezone setting matches the system's timezone behavior by testing it against a series of reference time points.

## Definition
```c
static int score_timezone(const char *tzname, struct tztry *tt)
```

## Detailed Description
This function assesses the compatibility of a given timezone with the system's local timezone behavior by comparing PostgreSQL's timezone calculations against the system's localtime() results for a series of test timestamps. It loads the specified timezone definition and tests it against multiple time points, returning a score that represents the number of successful matches. The function performs comprehensive validation including leap second checks, time component comparisons, and timezone abbreviation matching. A higher score indicates better compatibility, with -1 indicating complete incompatibility.

## Parameters / Member Variables
- `tzname`: Name of the timezone to evaluate (e.g., "America/New_York")
- `tt`: Pointer to a tztry structure containing test timestamps and related data for comparison

## Dependencies
- Functions called/Symbols referenced:
  - pg_load_tz
  - pg_tz_acceptable  
  - pg_localtime
  - compare_tm
  - localtime
  - strftime
  - strcmp
- Types referenced:
  - tztry
  - pg_time_t
  - pg_tm
  - pg_tz
- Called from (representative examples):
  - perfect_timezone_match
  - scan_available_timezones

## Notes and Other Information
- Returns -1 for completely unusable timezone settings (unrecognized name, uses leap seconds, etc.)
- Returns 0+ indicating the number of test times that matched successfully
- Test times are processed in order until the first mismatch is found
- Includes debug output when DEBUG_IDENTIFY_TIMEZONE is defined
- Validates both time values and timezone abbreviations for comprehensive matching
- Part of initdb's timezone detection mechanism to find the best system timezone match