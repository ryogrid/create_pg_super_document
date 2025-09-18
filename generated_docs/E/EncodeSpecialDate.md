# EncodeSpecialDate

## Location
src/backend/utils/adt/date.c: 294 - 308

## Overview
Converts PostgreSQL special date values (infinity and negative infinity) to their corresponding string representations.

## Definition
```c
void EncodeSpecialDate(DateADT dt, char *str)
```

## Detailed Description
The `EncodeSpecialDate` function handles the conversion of PostgreSQL's special date values to their string representations. PostgreSQL supports two special date values: negative infinity (representing a date earlier than any possible date) and positive infinity (representing a date later than any possible date). This function checks which special value is provided and copies the appropriate string constant to the output buffer. The function includes error handling for invalid arguments that don't represent recognized special date values.

## Parameters / Member Variables
- `dt`: DateADT value representing the special date to be encoded
- `str`: Character buffer where the resulting string representation will be stored

## Dependencies
- Functions called/Symbols referenced:
  - DATE_IS_NOBEGIN: Macro to check for negative infinity date
  - DATE_IS_NOEND: Macro to check for positive infinity date  
  - strcpy: Standard C string copy function
  - EARLY: String constant for negative infinity representation
  - LATE: String constant for positive infinity representation
  - elog: PostgreSQL error logging function
- Called from (representative examples):
  - [date_out](../d/date_out.md): Date output function for text format conversion
  - JsonEncodeDateTime: JSON encoding function for date/time values
  - PG_RETURN_TIMETZADT_P: Related time zone date handling

## Notes and Other Information
- This function only handles special infinite date values, not regular finite dates
- The EARLY and LATE constants likely contain string representations like "-infinity" and "infinity"
- Calling this function with a finite date value will result in an ERROR log message
- The function assumes the output buffer has sufficient space for the string constants
- Used primarily in date-to-string conversion routines and JSON serialization
- Part of PostgreSQL's comprehensive support for infinite date/time values