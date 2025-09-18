# time_timetz

## Location
src/backend/utils/adt/date.c: 2828 - 2853

## Overview
Converts a plain time (TimeADT) value to a time with time zone (TimeTzADT) by adding the current session's timezone information.

## Definition
```c
Datum time_timetz(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a conversion from a plain time data type to a time with time zone data type. It takes a TimeADT input parameter and constructs a TimeTzADT result by preserving the original time value and determining the appropriate timezone offset based on the current session timezone. The function uses the current date/time context to determine the timezone offset, which is necessary because timezone offsets can vary due to daylight saving time rules. The conversion process involves getting the current datetime, converting the time to a broken-down time structure, and then determining the timezone offset for the session timezone.

## Parameters / Member Variables
- Input parameter (via PG_GETARG_TIMEADT(0)): A TimeADT value representing the plain time to convert

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMEADT: Macro to extract TimeADT argument from function call
  - GetCurrentDateTime: Function to get current date/time information
  - time2tm: Function to convert TimeADT to broken-down time structure
  - DetermineTimeZoneOffset: Function to calculate timezone offset for given time and timezone
  - palloc: Memory allocation function
  - PG_RETURN_TIMETZADT_P: Macro to return TimeTzADT result
- Types used:
  - TimeADT: Plain time data type
  - TimeTzADT: Time with timezone data type
  - pg_tm: Broken-down time structure
  - fsec_t: Fractional seconds type
- Global variables accessed:
  - session_timezone: Current session's timezone setting
- Called from (representative examples):
  - executeDateTimeMethod: Used in JSON path execution for datetime method processing
  - castTimeToTimeTz: Used for casting time to time with timezone in JSON path operations

## Notes and Other Information
- The function allocates memory for the result TimeTzADT structure using palloc
- The timezone offset determination depends on the current session timezone setting
- The conversion preserves the exact time value while adding timezone context
- Located in src/backend/utils/adt/date.c alongside other date/time conversion functions
- The function follows PostgreSQL's standard function interface using PG_FUNCTION_ARGS
- Timezone offset calculation takes into account daylight saving time rules when applicable