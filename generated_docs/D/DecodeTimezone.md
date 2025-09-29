# DecodeTimezone

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:1500-1544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L1500-L1544)

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
  - [strtoint](../s/strtoint.md)
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

## Simplified Source

```c
int
DecodeTimezone(const char *str, int *tzp)
{
    int tz;
    int hr, min, sec = 0;
    char *cp;

    // Must start with '+' or '-' sign
    if (*str != '+' && *str != '-') {
        return DTERR_BAD_FORMAT;
    }

    // Parse hours
    hr = strtoint(str + 1, &cp, 10);
    if (errno == ERANGE) {
        return DTERR_TZDISP_OVERFLOW;
    }

    // Parse minutes and optionally seconds
    if (*cp == ':') {
        // Colon-delimited format: +HH:MM or +HH:MM:SS
        min = strtoint(cp + 1, &cp, 10);
        if (errno == ERANGE) return DTERR_TZDISP_OVERFLOW;

        if (*cp == ':') {
            // Also has seconds: +HH:MM:SS
            sec = strtoint(cp + 1, &cp, 10);
            if (errno == ERANGE) return DTERR_TZDISP_OVERFLOW;
        }
    }
    else if (*cp == '\0' && strlen(str) > 3) {
        // Run-together format: +HHMM
        min = hr % 100;
        hr = hr / 100;
    }
    else {
        min = 0;  // Just hours: +HH
    }

    // Validate ranges
    if (hr < 0 || hr > MAX_TZDISP_HOUR) return DTERR_TZDISP_OVERFLOW;
    if (min < 0 || min >= MINS_PER_HOUR) return DTERR_TZDISP_OVERFLOW;
    if (sec < 0 || sec >= SECS_PER_MINUTE) return DTERR_TZDISP_OVERFLOW;

    // Convert to total seconds
    tz = (hr * MINS_PER_HOUR + min) * SECS_PER_MINUTE + sec;
    if (*str == '-') {
        tz = -tz;  // Apply negative sign
    }

    // PostgreSQL uses negated timezone offsets internally
    *tzp = -tz;

    // Ensure we consumed the entire string
    if (*cp != '\0') {
        return DTERR_BAD_FORMAT;
    }

    return 0;
}
```