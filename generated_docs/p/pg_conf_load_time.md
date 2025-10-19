# pg_conf_load_time

## Location
[src/backend/utils/adt/timestamp.c:1642-1653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1642-L1653)

## Overview
Returns the timestamp when the PostgreSQL configuration was last loaded or reloaded, providing information about when configuration changes were last applied.

## Definition
```c
Datum pg_conf_load_time(PG_FUNCTION_ARGS)
```

## Detailed Description
The `pg_conf_load_time` function is a PostgreSQL built-in function that returns the timestamp with time zone indicating when the server configuration was last loaded or reloaded. This timestamp is updated whenever the configuration is reloaded (such as through SIGHUP signal, `pg_reload_conf()` function call, or server restart), making it useful for tracking configuration management and ensuring that configuration changes have been applied.

The function returns the value of the global variable `PgReloadTime`, which is updated whenever the PostgreSQL server processes configuration changes. This provides administrators with a way to verify when configuration modifications were last applied to the running server.

## Parameters / Member Variables
This function takes no parameters (uses `PG_FUNCTION_ARGS` macro for PostgreSQL function interface).

## Dependencies
- Functions called/Symbols referenced:
  - `PgReloadTime`: Global variable containing the configuration reload timestamp
  - `PG_RETURN_TIMESTAMPTZ`: PostgreSQL macro to return a timestamptz value

- Called from (representative examples):
  - SQL queries using the `pg_conf_load_time()` function
  - Configuration management and monitoring scripts
  - Administrative tools checking configuration status
  - Built-in function registry for SQL function dispatch

## Notes and Other Information
- The timestamp returned is with time zone (timestamptz type)
- Initially set to the server start time, then updated on each configuration reload
- Updated when configuration is reloaded via SIGHUP, `pg_reload_conf()`, or server restart
- Useful for verifying that configuration changes have been applied to the running server
- Helps administrators track configuration management activities
- Different from server start time - this reflects the last configuration reload event
- The function is defined in `src/backend/utils/adt/timestamp.c` at lines 1642-1653

## Simplified Source

```c
Datum
pg_conf_load_time(PG_FUNCTION_ARGS)
{
    // Return the configuration reload timestamp stored in global variable
    PG_RETURN_TIMESTAMPTZ(PgReloadTime);
}
```