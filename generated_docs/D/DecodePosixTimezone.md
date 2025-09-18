# DecodePosixTimezone

## Location
src/interfaces/ecpg/pgtypeslib/dt_common.c: 1545 - 1597

## Overview
DecodePosixTimezone parses POSIX-compatible timezone strings that combine timezone abbreviations with numeric offsets.

## Definition
```c
static int DecodePosixTimezone(char *str, int *tzp)
```

## Detailed Description
DecodePosixTimezone handles POSIX-style timezone specifications that combine a timezone abbreviation with a numeric offset, such as "PST-8:00" or "EST+5". The function separates the alphabetic timezone name from the numeric offset portion, uses DecodeSpecial to look up the timezone abbreviation value, and DecodeTimezone to parse the offset. It then combines these values to produce the final timezone offset. This function is part of the ECPG client library's datetime processing infrastructure.

## Parameters / Member Variables
- `str`: Input POSIX timezone string containing abbreviation and offset
- `tzp`: Output parameter to receive the combined timezone offset in seconds

## Dependencies
- Functions called/Symbols referenced:
  - [DecodeTimezone](DecodeTimezone.md)
  - [DecodeSpecial](DecodeSpecial.md)
  - MAXDATEFIELDS
  - DTZ (timezone type constant)
  - TZ (timezone type constant)
- Called from (representative examples):
  - [DecodeDateTime](DecodeDateTime.md) (in ECPG library)

## Notes and Other Information
- This is a static function specific to the ECPG client library
- Handles POSIX timezone format: abbreviation followed by offset (e.g., "PST-8:00")
- Combines timezone abbreviation value with numeric offset for final result
- Returns -1 on error, 0 on success
- Part of PostgreSQL's ECPG embedded SQL preprocessing system
- The timezone offset calculation follows the convention: -(abbreviation_value + numeric_offset)