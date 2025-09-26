# date2isoyear

## Location
[src/backend/utils/adt/timestamp.c:5222-5278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L5222-L5278)

## Overview
Returns the ISO 8601 year number for a given date, which may differ from the calendar year for dates in the first or last week of the year.

## Definition
```c
int date2isoyear(int year, int mon, int mday)
```

## Detailed Description
The `date2isoyear` function determines the ISO 8601 year for a given date. The ISO year follows the same logic as ISO week numbering: the first week of the year is the one containing the first Thursday. This means that:
- Days in early January may belong to the previous ISO year if they fall before the first Thursday
- Days in late December may belong to the next ISO year if they fall in a week that contains January 4th of the next year

The function uses similar logic to `date2isoweek` but returns the year component instead of the week number. It handles edge cases by checking if the date falls into the previous or next year's ISO year.

## Parameters / Member Variables
- `year`: The calendar year component of the date
- `mon`: The month component of the date (1-12)
- `mday`: The day component of the date (1-31)

## Dependencies
- Functions called/Symbols referenced:
  - [date2j](date2j.md) (converts date to Julian day number)
  - [j2day](../j/j2day.md) (converts Julian day to day of week)
- Called from (representative examples):
  - [extract_date](../e/extract_date.md)
  - [date2isoyearday](date2isoyearday.md)
  - [timestamp_part_common](../t/timestamp_part_common.md)
  - [timestamptz_part_common](../t/timestamptz_part_common.md)

## Notes and Other Information
- Returns an integer representing the ISO year, which may differ from the input calendar year
- Follows the year-zero-exists convention for zero or negative results
- Used in conjunction with ISO week calculations to provide complete ISO 8601 date functionality
- Essential for proper date arithmetic and formatting in PostgreSQL's temporal data types