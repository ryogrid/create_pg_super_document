# ValidateDate

## Location
[src/backend/utils/adt/datetime.c:2508-2589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L2508-L2589)

## Overview
ValidateDate is a date validation function that checks the validity of year, month, and day values in a parsed date structure, handling special cases like BC years, Julian calendars, 2-digit years, and day-of-year format.

## Definition

```c
int
ValidateDate(int fmask, bool isjulian, bool is2digits, bool bc,
			 struct pg_tm *tm)
```
## Detailed Description
ValidateDate performs comprehensive validation of date components stored in a pg_tm structure. The function handles several special date formats and edge cases:

1. **Year Processing**: Handles BC years by converting them to internal representation (1 BC becomes year 0), processes 2-digit years by mapping them to 1970-2069 range, and validates Julian calendar dates.

2. **Day-of-Year Conversion**: When DOY (day of year) is specified, converts it to month/day format using Julian day number calculations.

3. **Month and Day Validation**: Performs range checking for months (1-12) and days, including sophisticated validation that considers leap years and the actual number of days in each month.

The function uses a field mask (fmask) to determine which date components need validation, allowing partial date validation when only some fields are present.

## Parameters / Member Variables
- `fmask`: Bit mask indicating which date/time fields are present and need validation
- `isjulian`: Boolean flag indicating if the date uses Julian calendar
- `is2digits`: Boolean flag indicating if the year was input as 1 or 2 digits
- `bc`: Boolean flag indicating if the year is BC (Before Christ)
- `*tm`: Pointer to pg_tm structure containing the date components to validate
## Dependencies
- Functions called/Symbols referenced:
  - : Converts year/month/day to Julian day number
  - : Converts Julian day number to year/month/day
  - : Checks if a year is a leap year
  - : Macro for creating date/time field masks
  - : Array containing days per month for regular and leap years
- Called from (representative examples):
  - : Main datetime parsing function
  - : Time-only parsing function
  - : SQL date construction function
  - : Internal timestamp creation function

## Notes and Other Information
- Returns 0 for valid dates, or specific DTERR error codes for different types of validation failures
- Handles the absence of year zero in AD/BC notation by using internal representation where 1 BC = year 0
- Maps 2-digit years to 1970-2069 range following common conventions
- Performs day-of-month validation that accounts for leap years and varying month lengths
- Uses different error codes for different types of overflow (DTERR_FIELD_OVERFLOW vs DTERR_MD_FIELD_OVERFLOW)

## Simplified Source
```c
int ValidateDate(int fmask, bool isjulian, bool is2digits, bool bc, struct pg_tm *tm) {
    // Process year field if present
    if (fmask & DTK_M(YEAR)) {
        if (isjulian) {
            // Julian calendar - year is already correct
        } else if (bc) {
            // BC year: convert to internal representation (1 BC = year 0)
            if (tm->tm_year <= 0)
                return DTERR_FIELD_OVERFLOW;
            tm->tm_year = -(tm->tm_year - 1);
        } else if (is2digits) {
            // 2-digit year: map to 1970-2069 range
            if (tm->tm_year < 0)
                return DTERR_FIELD_OVERFLOW;
            if (tm->tm_year < 70)
                tm->tm_year += 2000;
            else if (tm->tm_year < 100)
                tm->tm_year += 1900;
        } else {
            // Regular AD year: no year zero allowed
            if (tm->tm_year <= 0)
                return DTERR_FIELD_OVERFLOW;
        }
    }

    // Convert day-of-year to month/day if specified
    if (fmask & DTK_M(DOY)) {
        j2date(date2j(tm->tm_year, 1, 1) + tm->tm_yday - 1,
               &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
    }

    // Validate month range (1-12)
    if (fmask & DTK_M(MONTH)) {
        if (tm->tm_mon < 1 || tm->tm_mon > MONTHS_PER_YEAR)
            return DTERR_MD_FIELD_OVERFLOW;
    }

    // Basic day range check (1-31)
    if (fmask & DTK_M(DAY)) {
        if (tm->tm_mday < 1 || tm->tm_mday > 31)
            return DTERR_MD_FIELD_OVERFLOW;
    }

    // Detailed day validation considering month length and leap years
    if ((fmask & DTK_DATE_M) == DTK_DATE_M) {
        if (tm->tm_mday > day_tab[isleap(tm->tm_year)][tm->tm_mon - 1])
            return DTERR_FIELD_OVERFLOW;
    }

    return 0; // All validations passed
}
```