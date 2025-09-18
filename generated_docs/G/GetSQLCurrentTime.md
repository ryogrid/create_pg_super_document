# GetSQLCurrentTime

## Location
src/backend/utils/adt/date.c: 342 - 361

## Overview
GetSQLCurrentTime implements the SQL CURRENT_TIME and CURRENT_TIME(n) functions, returning the current time of day with timezone information.

## Definition


## Detailed Description
This function retrieves the current time with timezone information and returns it as a TimeTzADT structure. It supports precision specification through the typmod parameter, which allows controlling the fractional seconds precision in the result. The function gets the current time using GetCurrentTimeUsec, converts it to the appropriate timezone-aware time format, and applies any precision adjustments specified by the typmod parameter.

## Parameters / Member Variables
- `typmod`: Type modifier that specifies the precision (number of fractional seconds digits) for the returned time value

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTimeUsec
  - palloc
  - tm2timetz
  - AdjustTimeForTypmod
- Types used:
  - TimeTzADT
  - pg_tm
  - fsec_t
- Called from (representative examples):
  - ExecEvalSQLValueFunction
  - PG_RETURN_TIMETZADT_P

## Notes and Other Information
- Implements SQL standard CURRENT_TIME and CURRENT_TIME(n) functions
- Returns a dynamically allocated TimeTzADT structure that must be managed by the caller
- The timezone component reflects the local timezone setting of the server
- Precision can be controlled via the typmod parameter, affecting fractional seconds display