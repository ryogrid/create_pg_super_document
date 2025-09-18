# date2isoyearday

## Location
[src/backend/utils/adt/timestamp.c:5279-5295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L5279-L5295)

## Overview
Returns the ISO 8601 day-of-year for a given Gregorian date, representing the ordinal day within the ISO year.

## Definition
```c
int date2isoyearday(int year, int mon, int mday)
```

## Detailed Description
The `date2isoyearday` function calculates the ISO 8601 day-of-year, which represents the ordinal day number within the ISO year (not the calendar year). This is computed by:
1. Converting the input date to a Julian day number
2. Determining the ISO year for the given date
3. Finding the Julian day number of the first day of that ISO year (week 1, day 1)
4. Calculating the difference to get the ordinal day position

Since ISO years can have 52 or 53 weeks, the possible return values range from 1 to 371 days (with 364 days in non-leap ISO years that have exactly 52 weeks).

## Parameters / Member Variables
- `year`: The calendar year component of the date
- `mon`: The month component of the date (1-12)
- `mday`: The day component of the date (1-31)

## Dependencies
- Functions called/Symbols referenced:
  - [date2j](date2j.md) (converts date to Julian day number)
  - [isoweek2j](../i/isoweek2j.md) (converts ISO year and week to Julian day)
  - [date2isoyear](date2isoyear.md) (determines ISO year for the date)
- Called from (representative examples):
  - timestamptz_cmp_internal

## Notes and Other Information
- Returns values from 1 to 371, representing the ordinal day within the ISO year
- The ISO year may differ from the calendar year for dates near year boundaries
- Used internally for timestamp comparison and ISO date calculations
- Provides a compact representation of a date's position within its ISO year
- Essential for ISO 8601 compliance in PostgreSQL's temporal operations