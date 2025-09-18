# PGTYPESdate_today

## Location
src/interfaces/ecpg/pgtypeslib/datetime.c: 148 - 156

## Overview
Retrieves the current system date and converts it to PostgreSQL's Julian date format.

## Definition
```c
void PGTYPESdate_today(date *d)
```

## Detailed Description
This function obtains the current system date using the GetCurrentDateTime utility function and converts it to PostgreSQL's internal Julian date representation. The function retrieves the current date components (year, month, day) in a tm structure, then uses the date2j function to convert these to a Julian day number, subtracting the reference date of January 1, 2000 to match PostgreSQL's date epoch. Error handling is included to check the success of the system date retrieval.

## Parameters / Member Variables
- `d`: Pointer to a date variable where the current date in Julian format will be stored

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentDateTime: Retrieves current system date and time into a tm structure
  - date2j: Converts year/month/day to Julian day number (called twice for calculation)
- Called from (representative examples):
  - rtoday: Informix compatibility wrapper function

## Notes and Other Information
- Part of PostgreSQL's ECPG (Embedded SQL in C) pgtypes library
- Only updates the output date if GetCurrentDateTime succeeds (errno == 0)
- Uses January 1, 2000 as the reference point for Julian date calculations
- The tm structure fields used are tm_year, tm_mon, and tm_mday from the system time
- Essential for applications that need to work with "today's date" in embedded SQL contexts
- Provides a standardized way to get the current date in PostgreSQL's internal format
- Error handling ensures the output date is only modified on successful date retrieval