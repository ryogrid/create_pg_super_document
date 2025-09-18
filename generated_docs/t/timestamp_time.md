# timestamp_time

## Location
[src/backend/utils/adt/date.c:1905-1934](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1905-L1934)

## Overview
The timestamp_time function extracts the time-of-day portion from a PostgreSQL timestamp, converting a full timestamp to a TimeADT value representing only the time component.

## Definition
```c
Datum timestamp_time(PG_FUNCTION_ARGS)
```

## Detailed Description
This function converts a timestamp to a time data type by extracting only the time-of-day information and discarding the date portion. It first validates that the timestamp is finite (not infinite or -infinite), then uses timestamp2tm to decompose the timestamp into its components. The resulting time value is calculated by combining hours, minutes, seconds, and fractional seconds into a single microsecond count since midnight. The function handles timezone-naive timestamps and includes proper error handling for out-of-range values.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the Timestamp argument to convert

## Dependencies
- Functions called/Symbols referenced:
  - Timestamp (data type)
  - TimeADT (data type)
  - PG_GETARG_TIMESTAMP (macro to extract timestamp argument)
  - PG_RETURN_TIMEADT (macro to return time result)
  - TIMESTAMP_NOT_FINITE (macro to check for infinite timestamps)
  - [timestamp2tm](timestamp2tm.md) (function to decompose timestamp)
  - [pg_tm](../p/pg_tm.md) (structure for time components)
  - fsec_t (fractional seconds type)
  - MINS_PER_HOUR, SECS_PER_MINUTE, USECS_PER_SEC (time conversion constants)
- Called from (representative examples):
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md) (in jsonpath_exec.c)

## Notes and Other Information
- Returns NULL for infinite timestamp values (positive or negative infinity)
- Includes comprehensive error handling with appropriate error codes and messages
- The time calculation manually combines time components rather than using modular arithmetic
- Located in src/backend/utils/adt/date.c at lines 1905-1934
- Part of PostgreSQL's date/time conversion function suite
- Preserves microsecond precision in the conversion process