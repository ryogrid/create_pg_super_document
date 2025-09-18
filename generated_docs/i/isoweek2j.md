# isoweek2j

## Location
src/backend/utils/adt/timestamp.c: 5116 - 5135

## Overview
Converts an ISO 8601 year and week number to the Julian day number corresponding to the first day (Monday) of that week.

## Definition


## Detailed Description
This function implements the conversion from ISO 8601 week date format to Julian day numbers. It calculates the Julian day number for the Monday of the specified ISO week in the given year. The algorithm works by first finding the Julian day for January 4th of the given year (which is guaranteed to be in the first ISO week of that year), then calculating the day-of-week offset to find the first Monday, and finally adding the appropriate number of weeks to reach the target week. This conversion is fundamental for implementing ISO week date arithmetic and conversions between different date representations.

## Parameters / Member Variables
-  (int): The ISO year (which may differ from the Gregorian calendar year for dates in early January or late December)
-  (int): The ISO week number (1-53, where week 1 contains January 4th)

## Dependencies
- Functions called/Symbols referenced:
  - date2j (converts Gregorian date to Julian day number)
  - j2day (converts Julian day to day-of-week number)
- Called from (representative examples):
  - do_to_timestamp (in formatting.c:4908)
  - isoweek2date (in timestamp.c:5138)
  - isoweekdate2date (in timestamp.c:5153)
  - date2isoyearday (in timestamp.c:5281)
  - timestamptz_cmp_internal (referenced in timestamp.h:138)

## Notes and Other Information
- The function is based on the ISO 8601 standard definition where week 1 is the first week containing January 4th
- Uses Julian day numbers as an intermediate representation for date calculations
- The algorithm accounts for the fact that ISO weeks always start on Monday
- Critical component for PostgreSQL's ISO week date support and date arithmetic operations
- The calculation  converts from 1-based week numbering to 0-based offset calculation
- Located in src/backend/utils/adt/timestamp.c:5116-5135