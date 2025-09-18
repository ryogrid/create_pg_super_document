# perfect_timezone_match

## Location
src/bin/initdb/findtimezone.c: 320 - 330

## Overview
Determines whether a given timezone name provides a perfect match to the system's localtime() behavior across all test time points.

## Definition
```c
static bool perfect_timezone_match(const char *tzname, struct tztry *tt)
```

## Detailed Description
This function serves as a wrapper around score_timezone() to determine if a timezone provides a complete match to the system's timezone behavior. It checks whether the timezone scoring function returns a score equal to the total number of test times, indicating that all test timestamps matched perfectly between PostgreSQL's timezone calculations and the system's localtime() results. This function is used to identify timezone candidates that provide exact compatibility with the system's timezone configuration.

## Parameters / Member Variables
- `tzname`: Name of the timezone to test for perfect matching (e.g., "America/New_York")
- `tt`: Pointer to a tztry structure containing test timestamps and the total count of test times

## Dependencies
- Functions called/Symbols referenced:
  - [score_timezone](../s/score_timezone.md)
  - tztry (structure type)
- Called from (representative examples):
  - [check_system_link_file](../c/check_system_link_file.md)

## Notes and Other Information
- Returns `true` only if the timezone matches all test time points perfectly
- Returns `false` if any test time point fails to match or if the timezone is invalid
- This is a stricter check than score_timezone() which returns partial match scores
- Used during timezone detection to identify exact matches before considering partial matches
- Part of initdb's strategy to find the most accurate timezone representation for the system