# timestamptz_timetz

## Location
src/backend/utils/adt/date.c: 2854 - 2885

## Overview
Converts a timestamp with time zone (TimestampTz) to a time with time zone (TimeTzADT) by extracting the time portion and preserving the timezone information.

## Definition
```c
Datum timestamptz_timetz(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a conversion from a timestamp with timezone data type to a time with timezone data type. It extracts only the time portion from a full timestamp while preserving the timezone information. The function handles edge cases like infinite timestamps (returns NULL) and validates that the timestamp is within the valid range. The conversion process involves breaking down the timestamp into its constituent parts (year, month, day, hour, minute, second, timezone) and then reconstructing only the time and timezone components into a TimeTzADT structure. This is useful when you need to work with just the time of day from a full timestamp while maintaining timezone awareness.

## Parameters / Member Variables
- Input parameter (via PG_GETARG_TIMESTAMP(0)): A TimestampTz value representing the timestamp with timezone to convert

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP: Macro to extract TimestampTz argument from function call
  - TIMESTAMP_NOT_FINITE: Macro to check if timestamp is infinite
  - timestamp2tm: Function to convert timestamp to broken-down time structure
  - palloc: Memory allocation function
  - tm2timetz: Function to convert broken-down time to TimeTzADT
  - PG_RETURN_TIMETZADT_P: Macro to return TimeTzADT result
  - ereport/errcode/errmsg: Error reporting functions
- Types used:
  - TimestampTz: Timestamp with timezone data type
  - TimeTzADT: Time with timezone data type
  - pg_tm: Broken-down time structure
  - fsec_t: Fractional seconds type
- Called from (representative examples):
  - executeDateTimeMethod: Used in JSON path execution for datetime method processing

## Notes and Other Information
- Returns NULL for infinite timestamps (both positive and negative infinity)
- Performs range validation and reports errors for out-of-range timestamps
- The function allocates memory for the result TimeTzADT structure using palloc
- Preserves both the time portion and timezone offset from the original timestamp
- Located in src/backend/utils/adt/date.c with other date/time conversion functions
- The function follows PostgreSQL's standard function interface using PG_FUNCTION_ARGS
- Error handling includes proper error codes for datetime value out of range conditions
- The timezone information is extracted from the timestamp itself, not from session settings