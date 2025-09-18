# DecodeISO8601Interval

## Location
src/interfaces/ecpg/pgtypeslib/interval.c: 112 - 325

## Overview
Decodes an ISO 8601 time interval string in either the "format with designators" or "alternative format" according to ISO 8601 standard sections 4.4.3.2 and 4.4.3.3.

## Definition
```c
int DecodeISO8601Interval(char *str, int *dtype, struct pg_itm_in *itm_in)
```

## Detailed Description
This function parses ISO 8601 interval strings and converts them into PostgreSQL's internal interval representation. It supports both the standard designator format (e.g., "P1D" for 1 day, "PT1H" for 1 hour, "P2Y6M7DT1H30M" for 2 years, 6 months, 7 days, 1 hour, 30 minutes) and the alternative format (e.g., "P0002-06-07T01:30:00").

The function implements a state machine that tracks whether it's currently parsing the date part (before 'T') or time part (after 'T') of the interval. It handles various date units (Y, M, W, D) and time units (H, M, S), as well as alternative formats with separators like hyphens and colons.

Key features include:
- Support for fractional values in any field (not just the least significant)
- Week fields ('W') can coexist with other units (exception from strict ISO 8601)
- Handles both basic and extended alternative formats
- Comprehensive error checking for malformed input

## Parameters / Member Variables
- `str`: Input string containing the ISO 8601 interval to be parsed
- `dtype`: Output parameter set to DTK_DELTA to indicate this is an interval type
- `itm_in`: Output parameter containing the parsed interval components (years, months, days, hours, minutes, seconds, microseconds)

## Dependencies
- Functions called/Symbols referenced:
  - ClearPgItmIn - initializes the interval structure
  - ParseISO8601Number - parses numeric values and fractional parts
  - AdjustYears, AdjustMonths, AdjustDays - adjust date components
  - AdjustMicroseconds, AdjustFractMicroseconds, AdjustFractYears, AdjustFractDays - adjust time components and fractional values
  - ISO8601IntegerWidth - validates integer field width for alternative formats
  - Constants: DTK_DELTA, DTERR_BAD_FORMAT, DTERR_FIELD_OVERFLOW, DAYS_PER_MONTH, USECS_PER_DAY, USECS_PER_HOUR, USECS_PER_MINUTE, USECS_PER_SEC
- Called from:
  - interval_in (src/backend/utils/adt/timestamp.c:938)
  - PGTYPESinterval_from_asc (src/interfaces/ecpg/pgtypeslib/interval.c:1033)

## Notes and Other Information
- Returns 0 on success, or a DTERR error code for malformed input
- The function deviates from strict ISO 8601 in two ways: allows week fields to coexist with other units, and permits decimals in fields other than the least significant unit
- Input must start with 'P' and be at least 2 characters long
- The 'T' character separates date and time components
- Alternative format parsing handles both basic (no separators) and extended (with separators) formats
- Comprehensive overflow checking prevents integer overflow in all adjustment operations