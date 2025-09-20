# interval_time

## Location
[src/backend/utils/adt/date.c:2012-2032](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2012-L2032)

## Overview
Converts an interval value to a time data type by extracting the fractional-day portion of the interval, effectively getting the time-of-day component.

## Definition

```c
Datum
interval_time(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that converts an Interval to a TimeADT by extracting the fractional-day portion of the interval. The function focuses specifically on the time component within a single day, ignoring the months field of the interval since it deals only with the intra-day time portion.

The function implements special handling for negative intervals by ensuring the result is always a positive time value within a 24-hour day. For example, an interval of "-2 hours" is converted to "22:00:00" by adding a full day's worth of microseconds to make it positive.

The function performs the following operations:
1. Extracts the interval argument from the function parameters
2. Checks for infinite intervals and reports an error if found
3. Calculates the time portion using modulo operation with microseconds per day
4. For negative results, adds a full day to normalize to positive time
5. Returns the resulting time value

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  -  (Interval*): The interval value to convert to time

## Dependencies
- Functions called/Symbols referenced:
  - Interval: Interval data structure for storing time periods
  - PG_GETARG_INTERVAL_P: Macro to extract Interval pointer argument
  - TimeADT: Time abstract data type for the result
  - INTERVAL_NOT_FINITE: Macro to check for infinite interval values
  - USECS_PER_DAY: Constant for microseconds per day (used for modulo and normalization)
  - PG_RETURN_TIMEADT: Macro to return TimeADT result

- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- The function produces the fractional-day portion of the interval, ignoring month components
- Negative intervals are normalized by subtracting the floor to produce positive time values
- Infinite intervals are not supported and will cause an error
- The modulo operation with USECS_PER_DAY extracts only the intra-day time component
- Examples: "-2 hours" becomes "22:00:00", "26 hours" becomes "02:00:00"
- The function effectively wraps time values to fit within a 24-hour period
- Located in src/backend/utils/adt/date.c:2012-2032