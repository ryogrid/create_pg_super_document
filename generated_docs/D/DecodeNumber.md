# DecodeNumber

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:1197-1305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L1197-L1305)

## Overview
DecodeNumber interprets a plain numeric field as a date value within the context of previously parsed fields, implementing PostgreSQL's intelligent date parsing logic.

## Definition

```c
static int
DecodeNumber(int flen, char *str, int fmask,
			 int *tmask, struct tm *tm, fsec_t *fsec, bool *is2digits, bool EuroDates)
```
## Detailed Description
This function is a central component of PostgreSQL's date/time parsing system that interprets numeric fields based on contextual information. It implements sophisticated logic to determine whether a numeric field represents a year, month, day, or time component based on:

1. **Field length and format**: Handles various numeric formats including decimals and concatenated fields
2. **Previously parsed fields**: Uses the fmask to understand what has already been parsed
3. **Date ordering preferences**: Respects the DateOrder setting (YMD, DMY, MDY)
4. **Text month context**: Behaves differently when a textual month name has been encountered

The function handles special cases like:
- Day-of-year format (3-digit numbers when only year is known)
- Fractional seconds parsing
- 2-digit year detection and marking
- Concatenated date/time fields
- Multiple date format ambiguities (DD-MON-YYYY, MON-DD-YYYY, YYYY-MON-DD)

## Parameters / Member Variables
- `flen`: Length of the numeric field being processed
- `*str`: String containing the numeric field to decode
- `fmask`: Boolean indicating if a textual month name was previously encountered
- `*tmask`: Bitmask indicating which date/time fields have already been parsed
- `*tm`: Pointer to bitmask that will be updated with the newly identified field type
- `*fsec`: Pointer to pg_tm structure where the parsed value will be stored
- `*is2digits`: Pointer to fractional seconds storage (used for decimal values)
- `EuroDates`: Pointer to boolean flag indicating if a 2-digit year was processed
## Dependencies
- Functions called/Symbols referenced:
  - [strtoint](../s/strtoint.md)
  - [DecodeNumberField](DecodeNumberField.md)
  - [ParseFractionalSecond](../P/ParseFractionalSecond.md)
  - DTK_M, DTK_DATE_M (field mask macros)
  - YEAR, MONTH, DAY, DOY (field type constants)
  - DATEORDER_YMD, DATEORDER_DMY, DATEORDER_MDY
  - DTERR_FIELD_OVERFLOW, DTERR_BAD_FORMAT (error constants)
  - struct pg_tm
  - fsec_t
- Called from (representative examples):
  - [DecodeDateTime](DecodeDateTime.md)
  - [DecodeTimeOnly](DecodeTimeOnly.md)
  - [DecodeDate](DecodeDate.md)

## Notes and Other Information
- This is a static function internal to datetime.c, not part of the public API
- Implements PostgreSQL's paranoid approach to date parsing to avoid ambiguities
- The function uses a state machine approach based on previously parsed fields
- Handles legacy 2-digit year formats with proper flagging for later adjustment
- Supports day-of-year parsing (e.g., 2023.365 for the 365th day of 2023)
- The logic prioritizes unambiguous interpretations and follows configured date ordering preferences
- Critical for supporting PostgreSQL's flexible date input format compatibility

## Simplified Source

```c
static int
DecodeNumber(int flen, char *str, bool haveTextMonth, int fmask,
             int *tmask, struct pg_tm *tm, fsec_t *fsec, bool *is2digits)
{
    int val;
    char *cp;
    int dterr;

    *tmask = 0;

    // Parse the numeric string
    val = strtoint(str, &cp, 10);
    if (errno == ERANGE) return DTERR_FIELD_OVERFLOW;
    if (cp == str) return DTERR_BAD_FORMAT;

    // Handle decimal point (fractional seconds or complex date formats)
    if (*cp == '.') {
        if (cp - str > 2) {
            // Multi-digit number with decimal: delegate to DecodeNumberField
            dterr = DecodeNumberField(flen, str, (fmask | DTK_DATE_M),
                                    tmask, tm, fsec, is2digits);
            return dterr < 0 ? dterr : 0;
        }
        // Parse fractional seconds
        dterr = ParseFractionalSecond(cp, fsec);
        if (dterr) return dterr;
    }
    else if (*cp != '\0') {
        return DTERR_BAD_FORMAT;
    }

    // Special case: day of year (3 digits when only year is known)
    if (flen == 3 && (fmask & DTK_DATE_M) == DTK_M(YEAR) && val >= 1 && val <= 366) {
        *tmask = (DTK_M(DOY) | DTK_M(MONTH) | DTK_M(DAY));
        tm->tm_yday = val;
        return 0;
    }

    // Determine field type based on what we've parsed so far
    switch (fmask & DTK_DATE_M) {
        case 0:
            // First field: decide based on length and DateOrder
            if (flen >= 3 || DateOrder == DATEORDER_YMD) {
                *tmask = DTK_M(YEAR);
                tm->tm_year = val;
            } else if (DateOrder == DATEORDER_DMY) {
                *tmask = DTK_M(DAY);
                tm->tm_mday = val;
            } else {
                *tmask = DTK_M(MONTH);
                tm->tm_mon = val;
            }
            break;

        case DTK_M(YEAR):
            // Second field after year: must be month
            *tmask = DTK_M(MONTH);
            tm->tm_mon = val;
            break;

        case DTK_M(MONTH):
            // After month: could be day or year depending on text month presence
            if (haveTextMonth) {
                *tmask = (flen >= 3 || DateOrder == DATEORDER_YMD) ?
                         DTK_M(YEAR) : DTK_M(DAY);
            } else {
                *tmask = DTK_M(DAY);
            }
            // Set appropriate tm field
            if (*tmask == DTK_M(YEAR)) tm->tm_year = val;
            else tm->tm_mday = val;
            break;

        case DTK_M(YEAR) | DTK_M(MONTH):
            // After year and month: must be day
            *tmask = DTK_M(DAY);
            tm->tm_mday = val;
            break;

        case DTK_M(DAY):
            // After day: must be month
            *tmask = DTK_M(MONTH);
            tm->tm_mon = val;
            break;

        case DTK_M(MONTH) | DTK_M(DAY):
            // After month and day: must be year
            *tmask = DTK_M(YEAR);
            tm->tm_year = val;
            break;

        case DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY):
            // All date fields present: this must be time
            dterr = DecodeNumberField(flen, str, fmask, tmask, tm, fsec, is2digits);
            return dterr < 0 ? dterr : 0;

        default:
            return DTERR_BAD_FORMAT;
    }

    // Mark 2-digit years for later adjustment
    if (*tmask == DTK_M(YEAR)) {
        *is2digits = (flen <= 2);
    }

    return 0;
}
```