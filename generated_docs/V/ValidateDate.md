# ValidateDate

## Location
src/backend/utils/adt/datetime.c: 2508 - 2589

## Overview
ValidateDate is a date validation function that checks the validity of year, month, and day values in a parsed date structure, handling special cases like BC years, Julian calendars, 2-digit years, and day-of-year format.

## Definition


## Detailed Description
ValidateDate performs comprehensive validation of date components stored in a pg_tm structure. The function handles several special date formats and edge cases:

1. **Year Processing**: Handles BC years by converting them to internal representation (1 BC becomes year 0), processes 2-digit years by mapping them to 1970-2069 range, and validates Julian calendar dates.

2. **Day-of-Year Conversion**: When DOY (day of year) is specified, converts it to month/day format using Julian day number calculations.

3. **Month and Day Validation**: Performs range checking for months (1-12) and days, including sophisticated validation that considers leap years and the actual number of days in each month.

The function uses a field mask (fmask) to determine which date components need validation, allowing partial date validation when only some fields are present.

## Parameters / Member Variables
- : Bit mask indicating which date/time fields are present and need validation
- : Boolean flag indicating if the date uses Julian calendar
- : Boolean flag indicating if the year was input as 1 or 2 digits
- : Boolean flag indicating if the year is BC (Before Christ)
- : Pointer to pg_tm structure containing the date components to validate

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