# isoweekdate2date

## Location
[src/backend/utils/adt/timestamp.c:5149-5166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L5149-L5166)

## Overview
Converts a complete ISO 8601 week date specification (ISO year, week number, and weekday) to the corresponding Gregorian calendar date.

## Definition

```c
void
isoweekdate2date(int isoweek, int wday, int *year, int *mon, int *mday)
```
## Detailed Description
This function converts a full ISO 8601 week date specification into a Gregorian calendar date. Unlike isoweek2date which only returns the Monday of a given week, this function can convert any specific day within an ISO week by accepting a weekday parameter. The function first calculates the Julian day for the Monday of the specified ISO week using isoweek2j, then applies an offset based on the weekday to find the exact date. The weekday conversion handles the difference between Gregorian week numbering (Sunday=1) and ISO week numbering (Monday=1). This function is particularly useful in date formatting operations where complete ISO week date strings need to be converted to standard dates.

## Parameters / Member Variables
-  (int): The ISO week number (1-53) within the year
-  (int): Day of week in Gregorian format (1=Sunday, 2=Monday, ..., 7=Saturday)
-  (int*): Pointer to ISO year value (input) and resulting Gregorian year (output)
-  (int*): Pointer to resulting month value (1-12)
-  (int*): Pointer to resulting day of month value (1-31)

## Dependencies
- Functions called/Symbols referenced:
  - [isoweek2j](isoweek2j.md) (converts ISO year/week to Julian day for Monday of that week)
  - [j2date](../j/j2date.md) (converts Julian day number to Gregorian date)
- Called from (representative examples):
  - [do_to_timestamp](../d/do_to_timestamp.md) (in formatting.c:4865)
  - timestamptz_cmp_internal (referenced in timestamp.h:140)

## Notes and Other Information
- Handles the conversion between Gregorian weekday numbering (Sunday=1) and ISO weekday numbering (Monday=1)
- The year parameter serves as both input (ISO year) and output (Gregorian year)
- The weekday offset calculation: if wday > 1, add (wday - 2), else add 6
- This handles the circular nature of weekdays where Sunday (1) becomes the 7th day in ISO numbering
- Essential for PostgreSQL's date formatting system, especially when parsing ISO week date formats
- The function modifies output parameters directly rather than returning a structure
- Located in src/backend/utils/adt/timestamp.c:5149-5166

## Simplified Source

```c
void
isoweekdate2date(int isoweek, int wday, int *year, int *mon, int *mday)
{
    int jday;

    // Get Julian day for Monday of the specified ISO week
    jday = isoweek2j(*year, isoweek);

    // Convert Gregorian weekday (Sunday=1) to ISO weekday offset
    // Monday=1 in ISO, so Sunday(1) becomes +6, Tue(3) becomes +1, etc.
    if (wday > 1)
        jday += wday - 2;  // Tuesday-Saturday: simple offset
    else
        jday += 6;         // Sunday: wraps to end of week

    // Convert Julian day to Gregorian date
    j2date(jday, year, mon, mday);
}
```