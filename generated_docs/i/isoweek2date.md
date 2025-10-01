# isoweek2date

## Location
[src/backend/utils/adt/timestamp.c:5136-5148](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L5136-L5148)

## Overview
Converts an ISO week number and year to the corresponding Gregorian calendar date (year, month, day) for the first day (Monday) of that week.

## Definition

```c
void
isoweek2date(int woy, int *year, int *mon, int *mday)
```
## Detailed Description
This function provides a convenient wrapper for converting ISO 8601 week dates to Gregorian calendar dates. It takes a week-of-year number and an ISO year, then calculates the corresponding Gregorian date for the Monday of that week. The implementation leverages the isoweek2j function to convert to Julian day numbers first, then uses j2date to convert to the final Gregorian date representation. This function is essential for date formatting operations and timestamp truncation that involve ISO week calculations.

## Parameters / Member Variables
-  (int): Week of year number (1-53 according to ISO 8601)
-  (int*): Pointer to ISO year value (input) and resulting Gregorian year (output)
-  (int*): Pointer to resulting month value (1-12)
-  (int*): Pointer to resulting day of month value (1-31)

## Dependencies
- Functions called/Symbols referenced:
  - [isoweek2j](isoweek2j.md) (converts ISO year/week to Julian day number)
  - [j2date](../j/j2date.md) (converts Julian day number to Gregorian date)
- Called from (representative examples):
  - [do_to_timestamp](../d/do_to_timestamp.md) (in formatting.c:4867)
  - [timestamp_trunc](../t/timestamp_trunc.md) (in timestamp.c:4663)
  - [timestamptz_trunc_internal](../t/timestamptz_trunc_internal.md) (in timestamp.c:4868)
  - timestamptz_cmp_internal (referenced in timestamp.h:139)

## Notes and Other Information
- The year parameter serves dual purpose: input ISO year and output Gregorian year
- The ISO year may differ from the Gregorian year for dates in early January or late December
- This function always returns the date for Monday of the specified ISO week
- Written by Karel Zak in 2000/08/07 according to the source comment
- Critical component in PostgreSQL's date/time formatting and truncation operations
- The function modifies the output parameters directly rather than returning a structure
- Located in src/backend/utils/adt/timestamp.c:5136-5148

## Simplified Source

```c
void
isoweek2date(int woy, int *year, int *mon, int *mday)
{
    // Convert ISO week to Julian day, then to Gregorian date
    j2date(isoweek2j(*year, woy), year, mon, mday);
}
```