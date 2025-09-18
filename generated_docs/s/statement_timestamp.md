# statement_timestamp

## Location
[src/backend/utils/adt/timestamp.c:1624-1629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1624-L1629)

## Overview
Returns the timestamp at which the current SQL statement began execution, providing a consistent time reference that remains constant throughout the execution of a single statement.

## Definition
```c
Datum statement_timestamp(PG_FUNCTION_ARGS)
```

## Detailed Description
The `statement_timestamp` function is a PostgreSQL built-in function that returns the start time of the current SQL statement as a timestamp with time zone. This function provides a consistent timestamp that does not change during the execution of a single statement, making it useful for scenarios where multiple references to the statement start time are needed within the same query.

The function is implemented as a simple wrapper around `GetCurrentStatementStartTimestamp()`, which retrieves the cached statement start time from the transaction state. This ensures that all calls to `statement_timestamp()` within the same statement return the exact same timestamp value.

## Parameters / Member Variables
This function takes no parameters (uses `PG_FUNCTION_ARGS` macro for PostgreSQL function interface).

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentStatementStartTimestamp](../G/GetCurrentStatementStartTimestamp.md): Retrieves the cached statement start timestamp
  - `PG_RETURN_TIMESTAMPTZ`: PostgreSQL macro to return a timestamptz value

- Called from (representative examples):
  - SQL queries using the `statement_timestamp()` function
  - Built-in function registry for SQL function dispatch

## Notes and Other Information
- The timestamp returned is with time zone (timestamptz type)
- The value remains constant throughout the execution of a single SQL statement
- This differs from `clock_timestamp()` which returns the current time on each call
- Commonly used in auditing and logging scenarios where consistent statement timing is required
- The function is defined in `src/backend/utils/adt/timestamp.c` at lines 1624-1629