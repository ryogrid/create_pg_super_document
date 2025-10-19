# make_time

## Location
[src/backend/utils/adt/date.c:1577-1604](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1577-L1604)

## Overview
Constructs a TIME value from individual hour, minute, and second components with validation and overflow checking.

## Definition

```c
Datum
make_time(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL constructor function that creates a TIME value from separate hour, minute, and second components. It validates the input values for range and overflow conditions, then converts them into the internal TimeADT representation (microseconds since midnight). The function performs thorough validation to ensure the resulting time value is within acceptable bounds and raises appropriate errors for invalid inputs.

The conversion algorithm matches the  function, ensuring consistency across PostgreSQL's time handling code. It calculates the total microseconds by first converting hours and minutes to total minutes, then to total seconds, then to total microseconds, and finally adding the fractional seconds converted to microseconds.

## Parameters / Member Variables
-  (int): Hour component (0-23)
-  (int): Minute component (0-59)  
-  (double): Second component with fractional seconds (0.0-59.999999)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts integer arguments for hour and minute
  - : Extracts double argument for seconds
  - : Validates time components for overflow conditions
  - : Rounds floating-point seconds to nearest integer microseconds
  - : Returns the constructed TimeADT value as a Datum
- Constants used:
  - : Minutes in an hour (60)
  - : Seconds in a minute (60)
  - : Microseconds in a second (1,000,000)
- Types used:
  - : Internal representation of time values as microseconds since midnight

## Notes and Other Information
- This function serves as a time constructor for SQL functions and expressions
- Performs comprehensive validation using  to prevent invalid time values
- The conversion algorithm explicitly matches  for consistency
- Handles fractional seconds with microsecond precision
- Raises  errors for out-of-range values
- Located in src/backend/utils/adt/date.c:1577-1604
- Used internally by PostgreSQL for constructing TIME values from components

## Simplified Source

```c
Datum make_time(PG_FUNCTION_ARGS) {
    int hour = PG_GETARG_INT32(0);
    int min = PG_GETARG_INT32(1);
    double sec = PG_GETARG_FLOAT8(2);

    // Validate time components for overflow
    if (float_time_overflows(hour, min, sec))
        ereport(ERROR, (errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
                       errmsg("time field value out of range: %d:%02d:%02g",
                              hour, min, sec)));

    // Convert to microseconds since midnight
    TimeADT time = (((hour * MINS_PER_HOUR + min) * SECS_PER_MINUTE) * USECS_PER_SEC)
                   + (int64) rint(sec * USECS_PER_SEC);

    PG_RETURN_TIMEADT(time);
}
```