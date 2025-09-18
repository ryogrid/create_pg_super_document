# clock_timestamp

## Location
[src/backend/utils/adt/timestamp.c:1630-1635](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1630-L1635)

## Overview
Returns the current timestamp at the moment of function call execution, providing a real-time clock reading that changes with each invocation.

## Definition
```c
Datum clock_timestamp(PG_FUNCTION_ARGS)
```

## Detailed Description
The `clock_timestamp` function is a PostgreSQL built-in function that returns the current timestamp with time zone at the exact moment the function is called. Unlike `statement_timestamp()` which returns a constant value throughout statement execution, `clock_timestamp()` provides a fresh timestamp on each invocation, making it suitable for measuring elapsed time within a statement or getting the most current time available.

The function is implemented as a simple wrapper around `GetCurrentTimestamp()`, which queries the system clock to obtain the current time. This ensures that each call to `clock_timestamp()` reflects the actual current time when the function executes.

## Parameters / Member Variables
This function takes no parameters (uses `PG_FUNCTION_ARGS` macro for PostgreSQL function interface).

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md): Retrieves the current system timestamp
  - `PG_RETURN_TIMESTAMPTZ`: PostgreSQL macro to return a timestamptz value

- Called from (representative examples):
  - SQL queries using the `clock_timestamp()` function
  - Built-in function registry for SQL function dispatch

## Notes and Other Information
- The timestamp returned is with time zone (timestamptz type)
- Each call returns the current system time, potentially different values within the same statement
- This differs from `statement_timestamp()` which returns a constant value per statement
- Useful for performance measurement, timing operations, and getting real-time timestamps
- Can be used multiple times in a query to measure execution time of different parts
- The function is defined in `src/backend/utils/adt/timestamp.c` at lines 1630-1635