# timestamptz_pl_interval

## Location
[src/backend/utils/adt/timestamp.c:3339-3347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L3339-L3347)

## Overview
Adds an interval to a timestamptz (timestamp with time zone) value, performing the calculation in the session timezone.

## Definition

```c
Datum
timestamptz_pl_interval(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL built-in function that implements the addition operation between a timestamp with time zone and an interval. It serves as a wrapper around the internal function , handling the PostgreSQL function call interface by extracting arguments and returning the result in the proper format. The operation is performed in the context of the current session's timezone setting.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  - Argument 0:  - The timestamp with time zone to which the interval will be added
  - Argument 1:  - Pointer to the interval structure containing the time span to add

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMPTZ (macro for extracting timestamptz argument)
  - PG_GETARG_INTERVAL_P (macro for extracting interval pointer argument)
  - [timestamptz_pl_interval_internal](timestamptz_pl_interval_internal.md) (internal implementation function)
  - PG_RETURN_TIMESTAMP (macro for returning timestamp result)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL operator '+')

## Notes and Other Information
- This function implements the '+' operator for timestamptz + interval operations in SQL
- The actual computation logic is delegated to 
- Located in src/backend/utils/adt/timestamp.c:3339-3347
- Returns a Datum containing the resulting timestamp with time zone

## Simplified Source

```c
Datum timestamptz_pl_interval(PG_FUNCTION_ARGS) {
    TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
    Interval *span = PG_GETARG_INTERVAL_P(1);

    // Delegate to internal function using session timezone
    PG_RETURN_TIMESTAMP(timestamptz_pl_interval_internal(timestamp, span, NULL));
}
```