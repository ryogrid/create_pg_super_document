# make_timestamp

## Location
[src/backend/utils/adt/timestamp.c:654-673](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L654-L673)

## Overview
PostgreSQL SQL function constructor that creates a timestamp (without timezone) from individual date and time components.

## Definition

```c
Datum
make_timestamp(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL SQL-callable function that constructs a timestamp value from separate year, month, day, hour, minute, and second components. This function serves as the SQL interface to the internal timestamp creation functionality.

The function extracts six parameters from the PostgreSQL function call arguments using the PG_GETARG macros, then delegates the actual timestamp construction to . It returns the result as a PostgreSQL Datum using the PG_RETURN_TIMESTAMP macro.

This function corresponds to the SQL function  that can be called from SQL queries.

## Parameters / Member Variables
- Function takes 6 PostgreSQL function arguments accessed via PG_GETARG macros:
  -  (int32): The year component
  -  (int32): The month component (1-12)
  -  (int32): The day component (1-31)
  -  (int32): The hour component (0-23)
  -  (int32): The minute component (0-59)
  -  (float8): The second component with fractional precision

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro)
  - PG_GETARG_FLOAT8 (macro)
  - [make_timestamp_internal](make_timestamp_internal.md)
  - PG_RETURN_TIMESTAMP (macro)
- Called from:
  - SQL queries (via function call mechanism)

## Notes and Other Information
- This is a PostgreSQL built-in SQL function accessible from SQL statements
- Uses the standard PostgreSQL function calling convention with PG_FUNCTION_ARGS
- All validation and error handling is performed by the underlying make_timestamp_internal function
- Returns a timestamp without timezone information
- Can be called from SQL as: SELECT make_timestamp(2023, 12, 25, 10, 30, 45.5);

## Simplified Source

```c
Datum make_timestamp(PG_FUNCTION_ARGS) {
    // Extract function arguments
    int32 year = PG_GETARG_INT32(0);
    int32 month = PG_GETARG_INT32(1);
    int32 mday = PG_GETARG_INT32(2);
    int32 hour = PG_GETARG_INT32(3);
    int32 min = PG_GETARG_INT32(4);
    float8 sec = PG_GETARG_FLOAT8(5);

    // Create timestamp using internal function
    Timestamp result = make_timestamp_internal(year, month, mday, hour, min, sec);

    // Return as PostgreSQL Datum
    PG_RETURN_TIMESTAMP(result);
}
```