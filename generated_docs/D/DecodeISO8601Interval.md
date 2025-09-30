# DecodeISO8601Interval

## Location
[src/interfaces/ecpg/pgtypeslib/interval.c:112-325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/interval.c#L112-L325)

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
  - [ClearPgItmIn](../C/ClearPgItmIn.md) - initializes the interval structure
  - [ParseISO8601Number](../P/ParseISO8601Number.md) - parses numeric values and fractional parts
  - [AdjustYears](../A/AdjustYears.md), AdjustMonths, AdjustDays - adjust date components
  - [AdjustMicroseconds](../A/AdjustMicroseconds.md), AdjustFractMicroseconds, AdjustFractYears, AdjustFractDays - adjust time components and fractional values
  - [ISO8601IntegerWidth](../I/ISO8601IntegerWidth.md) - validates integer field width for alternative formats
  - Constants: DTK_DELTA, DTERR_BAD_FORMAT, DTERR_FIELD_OVERFLOW, DAYS_PER_MONTH, USECS_PER_DAY, USECS_PER_HOUR, USECS_PER_MINUTE, USECS_PER_SEC
- Called from:
  - [interval_in](../i/interval_in.md) (src/backend/utils/adt/timestamp.c:938)
  - [PGTYPESinterval_from_asc](../P/PGTYPESinterval_from_asc.md) (src/interfaces/ecpg/pgtypeslib/interval.c:1033)

## Notes and Other Information
- Returns 0 on success, or a DTERR error code for malformed input
- The function deviates from strict ISO 8601 in two ways: allows week fields to coexist with other units, and permits decimals in fields other than the least significant unit
- Input must start with 'P' and be at least 2 characters long
- The 'T' character separates date and time components
- Alternative format parsing handles both basic (no separators) and extended (with separators) formats
- Comprehensive overflow checking prevents integer overflow in all adjustment operations

## Simplified Source

```c
int DecodeISO8601Interval(char *str, int *dtype, struct pg_itm_in *itm_in)
{
    bool datepart = true;  // Track if parsing date or time portion
    bool havefield = false;

    *dtype = DTK_DELTA;
    ClearPgItmIn(itm_in);

    // Must start with 'P' and be at least 2 characters
    if (strlen(str) < 2 || str[0] != 'P')
        return DTERR_BAD_FORMAT;

    str++;
    while (*str) {
        char *fieldstart;
        int64 val;
        double fval;
        char unit;
        int dterr;

        // 'T' separates date and time portions
        if (*str == 'T') {
            datepart = false;
            havefield = false;
            str++;
            continue;
        }

        // Parse numeric value (integer and fractional parts)
        fieldstart = str;
        dterr = ParseISO8601Number(str, &str, &val, &fval);
        if (dterr) return dterr;

        unit = *str++;

        if (datepart) {
            // Date units: Y(ears), M(onths), W(eeks), D(ays)
            switch (unit) {
                case 'Y':
                    if (!AdjustYears(val, 1, itm_in) || !AdjustFractYears(fval, 1, itm_in))
                        return DTERR_FIELD_OVERFLOW;
                    break;
                case 'M':
                    if (!AdjustMonths(val, itm_in) || !AdjustFractDays(fval, DAYS_PER_MONTH, itm_in))
                        return DTERR_FIELD_OVERFLOW;
                    break;
                case 'W':
                    if (!AdjustDays(val, 7, itm_in) || !AdjustFractDays(fval, 7, itm_in))
                        return DTERR_FIELD_OVERFLOW;
                    break;
                case 'D':
                    if (!AdjustDays(val, 1, itm_in) || !AdjustFractMicroseconds(fval, USECS_PER_DAY, itm_in))
                        return DTERR_FIELD_OVERFLOW;
                    break;
                // Handle alternative formats (basic and extended)
                case 'T':
                case '\0':
                case '-':
                    // Alternative format parsing logic (simplified)
                    // Handle formats like P2020-06-07T01:30:00 or P20200607T013000
                    return ProcessAlternativeFormat(fieldstart, unit, val, fval, itm_in, &str, &datepart, &havefield);
                default:
                    return DTERR_BAD_FORMAT;
            }
        } else {
            // Time units: H(ours), M(inutes), S(econds)
            switch (unit) {
                case 'H':
                    if (!AdjustMicroseconds(val, fval, USECS_PER_HOUR, itm_in))
                        return DTERR_FIELD_OVERFLOW;
                    break;
                case 'M':
                    if (!AdjustMicroseconds(val, fval, USECS_PER_MINUTE, itm_in))
                        return DTERR_FIELD_OVERFLOW;
                    break;
                case 'S':
                    if (!AdjustMicroseconds(val, fval, USECS_PER_SEC, itm_in))
                        return DTERR_FIELD_OVERFLOW;
                    break;
                // Handle alternative time formats
                case '\0':
                case ':':
                    return ProcessAlternativeTimeFormat(fieldstart, unit, val, fval, itm_in, &str, &havefield);
                default:
                    return DTERR_BAD_FORMAT;
            }
        }

        havefield = true;
    }

    return 0;
}
```