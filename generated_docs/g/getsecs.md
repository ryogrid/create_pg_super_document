# getsecs

## Location
src/timezone/localtime.c: 710 - 750

## Overview
Extracts a number of seconds in hh[:mm[:ss]] format from a timezone string and returns a pointer to the first character not part of the time specification.

## Definition

```c
static const char *
getsecs(const char *strp, int32 *const secsp)
```
## Detailed Description
The getsecs function parses time specifications from timezone strings, supporting flexible formats including hours, minutes, and seconds. It handles quasi-Posix rules that allow values like "M10.4.6/26" (equivalent to "02:00 on the first Sunday on or after 23 Oct"). The function extracts the time components sequentially:

1. Hours: Accepts values from 0 to (HOURSPERDAY * DAYSPERWEEK - 1) = 167 hours to support extended quasi-Posix notation
2. Minutes: Optional, accepts 0-59 minutes when preceded by ':'
3. Seconds: Optional, accepts 0-60 seconds (allowing leap seconds) when preceded by ':'

The total seconds are calculated and stored in the provided output parameter. If any parsing error occurs, the function returns NULL.

## Parameters / Member Variables
- : Pointer to the timezone string to parse
- : Pointer to int32 where the calculated total seconds will be stored

## Dependencies
- Functions called/Symbols referenced:
  - getnum (for parsing numeric components)
  - HOURSPERDAY (constant for hours per day)
  - DAYSPERWEEK (constant for days per week)
  - SECSPERHOUR (constant for seconds per hour)
  - MINSPERHOUR (constant for minutes per hour)
  - SECSPERMIN (constant for seconds per minute)
- Called from (representative examples):
  - getoffset

## Notes and Other Information
- This is a static function used internally within the timezone parsing subsystem
- The function allows leap seconds by accepting up to 60 seconds in the seconds field
- The extended hour range (0-167) supports quasi-Posix timezone rules that may specify times beyond 24 hours
- Returns NULL on any parsing error, making error handling straightforward for callers
- The function advances through the string character by character, making it suitable for sequential parsing of timezone specifications