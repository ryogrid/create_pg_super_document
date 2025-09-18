# datetimetz_timestamptz

## Location
src/backend/utils/adt/date.c: 2886 - 2926

## Overview
Combines a date (DateADT) and time with timezone (TimeTzADT) to create a timestamp with timezone (TimestampTz), properly handling timezone conversion and range validation.

## Definition
```c
Datum datetimetz_timestamptz(PG_FUNCTION_ARGS)
```

## Detailed Description
This function creates a timestamp with timezone by combining a date and a time with timezone. The result is stored in GMT (UTC), so the function adds the timezone offset from the TimeTzADT to properly convert the local time to UTC. The function handles special date values (negative and positive infinity) and performs comprehensive range checking since the date range is wider than the timestamp range. The conversion involves calculating microseconds from the epoch by combining the date (converted to microseconds), the time component, and adjusting for the timezone offset. Multiple validation steps ensure the result falls within valid timestamp ranges.

## Parameters / Member Variables
- Input parameter 0 (via PG_GETARG_DATEADT(0)): A DateADT value representing the date component
- Input parameter 1 (via PG_GETARG_TIMETZADT_P(1)): A TimeTzADT pointer representing the time with timezone component

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATEADT: Macro to extract DateADT argument from function call
  - PG_GETARG_TIMETZADT_P: Macro to extract TimeTzADT argument from function call
  - DATE_IS_NOBEGIN/DATE_IS_NOEND: Macros to check for infinite date values
  - TIMESTAMP_NOBEGIN/TIMESTAMP_NOEND: Macros to set infinite timestamp values
  - IS_VALID_TIMESTAMP: Macro to validate timestamp range
  - PG_RETURN_TIMESTAMP: Macro to return TimestampTz result
  - ereport/errcode/errmsg: Error reporting functions
- Constants used:
  - TIMESTAMP_END_JULIAN: Maximum Julian day for timestamps
  - POSTGRES_EPOCH_JDATE: PostgreSQL epoch Julian date
  - USECS_PER_DAY: Microseconds per day conversion factor
  - USECS_PER_SEC: Microseconds per second conversion factor
- Types used:
  - DateADT: Date data type
  - TimeTzADT: Time with timezone data type  
  - TimestampTz: Timestamp with timezone data type
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Properly handles infinite date values by propagating them to infinite timestamps
- Performs two-stage range validation: first for date boundaries, then for final timestamp validity
- The timezone adjustment converts local time to UTC for storage
- Date range is wider than timestamp range, requiring careful boundary checking
- Function includes detailed comments explaining the timezone conversion logic
- Located in src/backend/utils/adt/date.c with other date/time utility functions
- The function follows PostgreSQL's standard function interface using PG_FUNCTION_ARGS
- Originally implemented by Thomas Lockhart in March 2000
- Critical for ensuring timezone-aware timestamp creation from separate date and time components