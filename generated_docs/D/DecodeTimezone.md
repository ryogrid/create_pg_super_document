# DecodeTimezone

## Location
src/interfaces/ecpg/pgtypeslib/dt_common.c: 1500 - 1544

## Overview
DecodeTimezone parses numeric timezone offset strings and converts them into seconds offset values for timezone processing.

## Definition
```c
int DecodeTimezone(const char *str, int *tzp)
```

## Detailed Description
DecodeTimezone interprets string representations of numeric timezone offsets (like "+05:30" or "-0800") and converts them to seconds offset from UTC. The function handles both colon-delimited formats (HH:MM:SS or HH:MM) and run-together formats (HHMM). It performs comprehensive validation including range checking for hours, minutes, and seconds components. The resulting timezone offset is stored as seconds and negated to match PostgreSQL's internal timezone representation convention.

## Parameters / Member Variables
- `str`: Input timezone string to be decoded (must start with '+' or '-')
- `tzp`: Output parameter to receive the timezone offset in seconds (negated)

## Dependencies
- Functions called/Symbols referenced:
  - strtoint
  - DTERR_BAD_FORMAT
  - DTERR_TZDISP_OVERFLOW  
  - MAX_TZDISP_HOUR
  - MINS_PER_HOUR
  - SECS_PER_MINUTE
- Called from (representative examples):
  - [DecodeDateTime](DecodeDateTime.md)
  - [DecodeTimeOnly](DecodeTimeOnly.md)
  - [parse_sane_timezone](../p/parse_sane_timezone.md)
  - [DecodePosixTimezone](DecodePosixTimezone.md) (in ECPG)

## Notes and Other Information
- Accepts formats like +HH:MM:SS, +HH:MM, +HHMM, -HH:MM:SS, -HH:MM, -HHMM
- Performs strict range validation on hour (0-MAX_TZDISP_HOUR), minute (0-59), and second (0-59) components
- Returns negated timezone offset to match PostgreSQL's internal representation
- Used extensively in datetime parsing throughout PostgreSQL backend and ECPG client library
- Handles overflow conditions with specific DTERR_TZDISP_OVERFLOW error code