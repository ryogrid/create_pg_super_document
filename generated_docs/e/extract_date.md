# extract_date

## Location
src/backend/utils/adt/date.c: 1066 - 1245

## Overview
Extracts specified fields (like year, month, day, etc.) from a PostgreSQL date value and returns the result as a numeric value.

## Definition
```c
Datum extract_date(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the EXTRACT functionality for PostgreSQL date data types. It takes a text unit specification (such as 'year', 'month', 'day', 'quarter', etc.) and a date value, then extracts the specified component and returns it as a numeric result.

The function handles a wide variety of date components including:
- Basic components: day, month, year
- Derived components: quarter, week, decade, century, millennium
- Special components: Julian day, ISO year, day of week, day of year, epoch
- Infinite date handling: properly handles positive and negative infinity dates

For infinite dates, oscillating units (day, month, quarter, week, dow, isodow, doy) return NULL, while monotonically-increasing units (year, decade, century, millennium, julian, isoyear, epoch) return appropriate infinity values.

The function uses PostgreSQL's internal date conversion functions and follows SQL standard semantics for date component extraction.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: Text specification of the unit to extract (units)
- `PG_GETARG_DATEADT(1)`: The date value to extract from (date)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP`, `PG_GETARG_DATEADT` - Argument extraction macros
  - `downcase_truncate_identifier` - String processing for unit names
  - `DecodeUnits`, `DecodeSpecial` - Unit parsing functions
  - `j2date` - Julian to Gregorian date conversion
  - `date2isoweek`, `date2isoyear` - ISO week/year calculations
  - `j2day` - Julian to day-of-week conversion
  - `date2j` - Gregorian to Julian date conversion
  - `int64_to_numeric` - Numeric result conversion
  - `DATE_NOT_FINITE`, `DATE_IS_NOBEGIN` - Infinite date checks
  - Various `DTK_*` constants for date/time field types
  - `POSTGRES_EPOCH_JDATE`, `UNIX_EPOCH_JDATE`, `SECS_PER_DAY` - Epoch constants
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of PostgreSQL's EXTRACT function infrastructure for date types
- Handles special cases for infinite dates with appropriate semantics
- Supports both standard SQL and PostgreSQL-specific date components
- Located in src/backend/utils/adt/date.c:1066-1245
- Returns results as PostgreSQL numeric type to handle large values
- Follows same logic patterns as timestamp extraction functions
- BC (Before Christ) years are handled with special adjustment logic (no year 0)