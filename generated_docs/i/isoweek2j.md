# isoweek2j

## Location
[src/backend/utils/adt/timestamp.c:5116-5135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L5116-L5135)

## Overview
Converts an ISO 8601 year and week number to the Julian day number corresponding to the first day (Monday) of that week.

## Definition

```c
int
isoweek2j(int year, int week)
```
## Detailed Description
This function implements the conversion from ISO 8601 week date format to Julian day numbers. It calculates the Julian day number for the Monday of the specified ISO week in the given year. The algorithm works by first finding the Julian day for January 4th of the given year (which is guaranteed to be in the first ISO week of that year), then calculating the day-of-week offset to find the first Monday, and finally adding the appropriate number of weeks to reach the target week. This conversion is fundamental for implementing ISO week date arithmetic and conversions between different date representations.

## Parameters / Member Variables
-  (int): The ISO year (which may differ from the Gregorian calendar year for dates in early January or late December)
-  (int): The ISO week number (1-53, where week 1 contains January 4th)

## Dependencies
- Functions called/Symbols referenced:
  - [date2j](../d/date2j.md) (converts Gregorian date to Julian day number)
  - [j2day](../j/j2day.md) (converts Julian day to day-of-week number)
- Called from (representative examples):
  - [do_to_timestamp](../d/do_to_timestamp.md) (in formatting.c:4908)
  - [isoweek2date](isoweek2date.md) (in timestamp.c:5138)
  - [isoweekdate2date](isoweekdate2date.md) (in timestamp.c:5153)
  - [date2isoyearday](../d/date2isoyearday.md) (in timestamp.c:5281)
  - timestamptz_cmp_internal (referenced in timestamp.h:138)

## Notes and Other Information
- The function is based on the ISO 8601 standard definition where week 1 is the first week containing January 4th
- Uses Julian day numbers as an intermediate representation for date calculations
- The algorithm accounts for the fact that ISO weeks always start on Monday
- Critical component for PostgreSQL's ISO week date support and date arithmetic operations
- The calculation  converts from 1-based week numbering to 0-based offset calculation
- Located in src/backend/utils/adt/timestamp.c:5116-5135

## Simplified Source

```c
int
isoweek2j(int year, int week)
{
    int day0, day4;

    // Find Julian day for January 4th (always in ISO week 1)
    day4 = date2j(year, 1, 4);

    // Calculate offset to first Monday of the year
    day0 = j2day(day4 - 1);

    // Calculate Julian day for Monday of the specified week
    return ((week - 1) * 7) + (day4 - day0);
}
```