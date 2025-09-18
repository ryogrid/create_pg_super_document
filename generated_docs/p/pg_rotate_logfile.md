# pg_rotate_logfile

## Location
src/backend/storage/ipc/signalfuncs.c: 280 - 291

## Overview
Triggers log file rotation in PostgreSQL by sending a signal to the postmaster process.

## Definition
```c
Datum pg_rotate_logfile(PG_FUNCTION_ARGS)
```

## Detailed Description
The `pg_rotate_logfile` function is a PostgreSQL SQL-callable function that initiates log file rotation when the logging collector is active. It serves as the backend implementation for the SQL function `pg_rotate_logfile()` which can be called by database users with appropriate permissions to force a log file rotation.

The function first checks if the logging collector (`Logging_collector` global variable) is enabled. If logging collection is not active, it issues a warning and returns false. If logging is active, it sends the `PMSIGNAL_ROTATE_LOGFILE` signal to the postmaster process via `SendPostmasterSignal()` and returns true to indicate successful initiation of the rotation request.

This function provides a programmatic way to rotate log files on demand, which is useful for log management and ensuring that log files dont grow too large.

## Parameters / Member Variables
This function takes no explicit parameters (uses `PG_FUNCTION_ARGS` macro for PostgreSQL function interface).

## Dependencies
- Functions called/Symbols referenced:
  - `SendPostmasterSignal` - Sends inter-process signals to the postmaster
  - `PMSIGNAL_ROTATE_LOGFILE` - Signal constant for log rotation request
  - `Logging_collector` - Global variable indicating if log collection is active
  - `ereport` - PostgreSQL error reporting function
  - `[errmsg](../e/errmsg.md)` - Error message formatting function

- Called from (representative examples):
  - No direct code references found in the analyzed codebase (likely called via SQL interface)

## Notes and Other Information
- Permission checking for this function is managed through PostgreSQLs normal GRANT system
- The function is located in `src/backend/storage/ipc/signalfuncs.c:280-291`
- Returns a boolean value indicating success (true) or failure (false) of the rotation request
- This is a signal-based operation - the function initiates the request but actual log rotation is performed asynchronously by the postmaster process
- The function will issue a warning if called when log collection is not active, making it safe to call in various configurations