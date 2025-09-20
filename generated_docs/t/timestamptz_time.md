# timestamptz_time

## Location
[src/backend/utils/adt/date.c:1935-1965](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1935-L1965)

## Overview
Converts a timestamp with time zone (timestamptz) value to a time-only value, extracting just the time portion and discarding the date and timezone information.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
The  function is a PostgreSQL built-in function that extracts the time component from a timestamptz (timestamp with timezone) value. It converts the input timestamp to the local time representation and then calculates the time of day in microseconds since midnight. The function handles timezone conversions by using the  function to decompose the timestamp into its constituent parts, then reconstructs just the time portion as a TimeADT value.

The function performs the following operations:
1. Retrieves the timestamptz input parameter
2. Checks for non-finite timestamps (infinity, -infinity) and returns NULL if found
3. Converts the timestamp to broken-down time components using timezone information
4. Calculates the time in microseconds from the hour, minute, second, and fractional second components
5. Returns the result as a TimeADT value

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : Input timestamptz value to convert

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP: Macro to extract timestamptz argument
  - TimeADT: Time data type for storing time values
  - [pg_tm](../p/pg_tm.md): Structure for broken-down time components
  - fsec_t: Type for fractional seconds
  - TIMESTAMP_NOT_FINITE: Macro to check for infinite timestamp values
  - [timestamp2tm](timestamp2tm.md): Function to convert timestamp to broken-down time with timezone
  - SECS_PER_MINUTE: Constant for seconds per minute conversion
  - MINS_PER_HOUR: Constant for minutes per hour conversion
  - USECS_PER_SEC: Constant for microseconds per second conversion
  - PG_RETURN_TIMEADT: Macro to return TimeADT result

- Called from (representative examples):
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md): Used in JSON path execution for datetime method calls

## Notes and Other Information
- The function returns NULL for non-finite timestamps (infinity/-infinity)
- Timezone information is used during conversion but discarded in the final result
- The result represents time as microseconds since midnight
- Error handling includes checking for timestamp values that are out of range
- The conversion process involves timezone-aware timestamp decomposition followed by time-only reconstruction
- Located in src/backend/utils/adt/date.c:1935-1965