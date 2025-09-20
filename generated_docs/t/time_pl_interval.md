# time_pl_interval

## Location
[src/backend/utils/adt/date.c:2052-2074](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2052-L2074)

## Overview
The  function adds an interval to a time value, performing time arithmetic while handling overflow and ensuring the result remains within a valid 24-hour day range.

## Definition

```c
Datum
time_pl_interval(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the PostgreSQL operator for adding an interval to a time data type (TIME + INTERVAL). It extracts a TimeADT value and an Interval pointer from the function arguments, performs the addition, and handles day overflow by using modulo arithmetic to wrap the result within a 24-hour period. The function ensures that infinite intervals are rejected with an appropriate error message, and negative results are normalized to positive values within the day range.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - The base time value to add the interval to
  - Argument 1:  - Pointer to the interval structure to be added

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMEADT
  - PG_GETARG_INTERVAL_P
  - INTERVAL_NOT_FINITE
  - PG_RETURN_TIMEADT
  - ereport (for error handling)
- Data types used:
  - TimeADT
  - Interval
  - USECS_PER_DAY
  - INT64CONST
- Called from (representative examples):
  - No direct references found (likely called via SQL operator system)

## Notes and Other Information
- The function performs modulo arithmetic using  to ensure the result stays within a 24-hour range
- Infinite intervals are explicitly rejected with error code 
- Negative results are normalized by adding  to ensure a positive time value
- This function is typically invoked through PostgreSQL's operator system rather than direct function calls
- Located in 