# date2isoweek

## Location
[src/backend/utils/adt/timestamp.c:5167-5221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L5167-L5221)

## Overview
Returns the ISO week number of the year for a given date, following the ISO 8601 standard for week numbering.

## Definition


## Detailed Description
The  function calculates the ISO week number for a given date according to the ISO 8601 standard. In this system, weeks start on Monday, and the first week of the year is the one that contains the first Thursday of the year. This means that some days in early January may belong to the last week of the previous year, and some days in late December may belong to the first week of the next year.

The algorithm works by:
1. Converting the input date to a Julian day number
2. Finding the fourth day (Thursday) of the current year
3. Determining the Monday of the week containing that Thursday
4. Calculating the week number based on the difference from that Monday
5. Handling edge cases where the date falls into the previous or next year's week numbering

## Parameters / Member Variables
- : The year component of the date
- : The month component of the date (1-12)
- : The day component of the date (1-31)

## Dependencies
- Functions called/Symbols referenced:
  - [date2j](date2j.md) (converts date to Julian day number)
  - j2day (converts Julian day to day of week)
- Called from (representative examples):
  - [extract_date](../e/extract_date.md)
  - [timestamp_trunc](../t/timestamp_trunc.md)
  - [timestamptz_trunc_internal](../t/timestamptz_trunc_internal.md)
  - [timestamp_part_common](../t/timestamp_part_common.md)
  - [timestamptz_part_common](../t/timestamptz_part_common.md)

## Notes and Other Information
- The function returns an integer representing the week number (1-53)
- Implements ISO 8601 week numbering standard where weeks start on Monday
- The first week of the year is the one containing the first Thursday
- Edge case handling ensures correct week numbers for dates at year boundaries
- Used internally by various timestamp and date extraction functions in PostgreSQL