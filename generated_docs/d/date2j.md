# date2j

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:581-605](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L581-L605)

## Overview
Converts calendar date (year, month, day) to Julian day number, providing a numerically accurate and computationally simple representation commonly used in astronomical applications.

## Definition


## Detailed Description
date2j implements a calendar time to Julian date conversion algorithm that accurately converts between Julian day and calendar date for all non-negative Julian days (from Nov 24, -4713 onwards). The function has been rewritten to eliminate overflow problems and now correctly handles all Julian day counts from 0 to 2147483647 (Nov 24, -4713 to Jun 3, 5874898) for 32-bit integers.

The algorithm uses a sophisticated calculation that accounts for leap years and varying month lengths by adjusting the month and year values before performing the core Julian day calculation. It handles the Gregorian calendar reform by incorporating century-based leap year corrections. The function can produce valid negative Julian dates significantly before Nov 24, -4713, extending back to Nov 1, -4713 as required by IS_VALID_JULIAN().

## Parameters / Member Variables
- : The calendar year (can be negative for years BC)
- : The month number (1-12, where 1 = January)
- : The day of the month (1-31)

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic arithmetic operations)
- Called from (representative examples):
  - [date_in](date_in.md) (date input parsing)
  - [make_date](../m/make_date.md) (date construction function)
  - [DecodeDateTime](../D/DecodeDateTime.md) (date/time parsing)
  - [timestamp_part_common](../t/timestamp_part_common.md) (timestamp extraction)
  - [date2isoweek](date2isoweek.md) (ISO week calculations)
  - ValidateDate (date validation)

## Notes and Other Information
- Returns the Julian day number as an integer
- The algorithm adjusts months > 2 and months <= 2 differently to handle leap year calculations
- Uses integer arithmetic with specific constants (4800, 4799, 7834, 256, etc.) derived from the Julian calendar algorithm
- Central to PostgreSQL's internal date representation and calculations
- The function is numerically stable and avoids overflow issues that plagued earlier implementations
- Essential for date arithmetic, comparisons, and conversions throughout the PostgreSQL date/time system
- Works correctly with the Gregorian calendar and accounts for the transition from Julian calendar