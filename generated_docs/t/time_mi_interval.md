# time_mi_interval

## Location
[src/backend/utils/adt/date.c:2075-2097](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2075-L2097)

## Overview
The  function subtracts an interval from a time value, performing time arithmetic while handling underflow and ensuring the result remains within a valid 24-hour day range.

## Definition

```c
Datum
time_mi_interval(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the PostgreSQL operator for subtracting an interval from a time data type (TIME - INTERVAL). It extracts a TimeADT value and an Interval pointer from the function arguments, performs the subtraction, and handles day underflow by using modulo arithmetic to wrap the result within a 24-hour period. The function ensures that infinite intervals are rejected with an appropriate error message, and negative results are normalized to positive values within the day range.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - The base time value to subtract the interval from
  - Argument 1:  - Pointer to the interval structure to be subtracted

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
- The implementation is nearly identical to  except it performs subtraction instead of addition
- Located in

## Simplified Source

```c
Datum time_mi_interval(PG_FUNCTION_ARGS) {
    TimeADT time = PG_GETARG_TIMEADT(0);
    Interval *span = PG_GETARG_INTERVAL_P(1);
    TimeADT result;

    // Check for infinite interval
    if (INTERVAL_NOT_FINITE(span))
        ereport(ERROR, "cannot subtract infinite interval from time");

    // Subtract interval from time
    result = time - span->time;

    // Keep result within 24-hour day range
    result -= result / USECS_PER_DAY * USECS_PER_DAY;
    if (result < 0)
        result += USECS_PER_DAY;

    PG_RETURN_TIMEADT(result);
}
```