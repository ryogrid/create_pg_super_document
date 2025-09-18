# stringrule

## Location
[src/timezone/zic.c:2716-2796](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L2716-L2796)

## Overview
Converts a timezone rule to its string representation in POSIX timezone format, handling both day-of-month and day-of-week rule types with time offset calculations.

## Definition


## Detailed Description
The  function generates a POSIX timezone rule string from a PostgreSQL timezone rule structure. It handles two main types of daylight saving time transition rules:

1. **Day-of-month rules (DC_DOM)**: Fixed calendar dates like "March 15th"
2. **Day-of-week rules (DC_DOWGEQ/DC_DOWLEQ)**: Relative dates like "first Sunday in March" or "last Sunday in October"

The function formats the rule into POSIX timezone format (e.g., "M3.2.0" for second Sunday in March) and applies time offset corrections based on whether the rule time is in UTC, standard time, or wall clock time. It returns a compatibility year indicating the earliest POSIX version that supports the generated rule format.

## Parameters / Member Variables
- : Output buffer where the formatted rule string will be written
- : Pointer to the rule structure containing the transition rule definition
- : The daylight saving time offset to apply
- : The standard time offset from UTC

## Dependencies
- Functions called/Symbols referenced:
  -  (standard library function for string formatting)
  -  (formats time offset into string)
  -  (array containing days per month)
- Called from (representative examples):
  -  (generates complete timezone string representations)

## Notes and Other Information
- Returns -1 for unsupported rule types (like February 29th in non-leap years)
- Returns a compatibility year (0, 1994, or 2013) indicating POSIX version requirements
- Handles leap year considerations by rejecting February 29th rules
- Optimizes output by omitting the 'J' prefix for January and February dates
- Applies complex time offset calculations for UTC, standard time, and wall clock time conversions
- Part of PostgreSQL's timezone compilation system (zic) for generating binary timezone files