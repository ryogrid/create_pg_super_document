# cmpDateToTimestampTz

## Location
src/backend/utils/adt/jsonpath_exec.c: 3699 - 3709

## Overview
A static helper function that compares a date value to a timestamptz (timestamp with timezone) value with timezone validation during JSON path execution.

## Definition
```c
static int cmpDateToTimestampTz(DateADT date1, TimestampTz tstz2, bool useTz)
```

## Detailed Description
This function performs a comparison between a date and timestamptz value, but first validates that timezone usage is properly enabled through checkTimezoneIsUsedForCast. Unlike cmpDateToTimestamp, this function explicitly requires timezone context since it deals with timestamptz values. After validation, it delegates the actual comparison to PostgreSQL's internal date_cmp_timestamptz_internal function, ensuring proper handling of timezone-aware timestamp comparisons.

## Parameters / Member Variables
- `date1`: The date value to compare (DateADT type)
- `tstz2`: The timezone-aware timestamp value to compare against (TimestampTz type)
- `useTz`: Boolean flag indicating whether timezone usage is enabled for the comparison

## Dependencies
- Functions called/Symbols referenced:
  - [checkTimezoneIsUsedForCast](checkTimezoneIsUsedForCast.md) (timezone validation)
  - [date_cmp_timestamptz_internal](../d/date_cmp_timestamptz_internal.md) (core comparison function)
  - DateADT (date data type)
  - TimestampTz (timezone-aware timestamp data type)
- Called from (representative examples):
  - [compareDatetime](compareDatetime.md) (multiple locations for datetime comparisons)

## Notes and Other Information
- Located in src/backend/utils/adt/jsonpath_exec.c:3699-3709
- Part of PostgreSQL's JSON path execution engine for timezone-aware temporal comparisons
- Requires explicit timezone usage validation before performing the comparison
- Returns an integer comparison result (negative, zero, or positive)
- Critical for maintaining temporal data integrity when comparing dates to timezone-aware timestamps
- Will raise an error if timezone usage is not enabled, preventing potentially incorrect comparisons