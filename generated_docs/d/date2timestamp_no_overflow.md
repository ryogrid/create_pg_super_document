# date2timestamp_no_overflow

## Location
src/backend/utils/adt/date.c: 720 - 742

## Overview
Converts a date value to a double precision timestamp without throwing overflow errors, specifically designed for statistical estimation purposes.

## Definition


## Detailed Description
This function is chartered to produce a double value that is numerically equivalent to the corresponding Timestamp value, if the date is in the valid range of Timestamps, but in any case not throw an overflow error. The function can safely handle any date value since the numerical range of double is greater than that of non-erroneous timestamps. The results are currently only used for statistical estimation purposes within PostgreSQL's query planner.

The function handles special date values:
- For negative infinity dates (NOBEGIN), returns -DBL_MAX
- For positive infinity dates (NOEND), returns DBL_MAX
- For regular dates, converts by multiplying the date value (days since 2000) by microseconds per day

## Parameters / Member Variables
- `dateVal`: A DateADT value representing days since January 1, 2000 (PostgreSQL's epoch for dates)

## Dependencies
- Functions called/Symbols referenced:
  - DATE_IS_NOBEGIN (macro to check for negative infinity date)
  - DATE_IS_NOEND (macro to check for positive infinity date)
  - USECS_PER_DAY (constant for microseconds per day conversion)
  - DateADT (PostgreSQL's date type)
- Called from (representative examples):
  - convert_timevalue_to_scalar (in query planner statistics)

## Notes and Other Information
- The function is specifically designed to avoid overflow exceptions that could occur with regular timestamp conversion
- Uses double precision floating point to handle the full range of possible date values
- The conversion formula: result = dateVal * USECS_PER_DAY where dateVal is days since 2000-01-01
- Primarily used in PostgreSQL's statistics and query planning subsystem rather than for general date/timestamp operations
- Located in src/backend/utils/adt/date.c:720-742