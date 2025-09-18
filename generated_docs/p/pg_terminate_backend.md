# pg_terminate_backend

## Location
src/backend/storage/ipc/signalfuncs.c: 216 - 259

## Overview
SQL-callable function that terminates a PostgreSQL backend process by sending a SIGTERM signal, with optional timeout-based waiting for confirmation of termination.

## Definition
```c
Datum pg_terminate_backend(PG_FUNCTION_ARGS)
```

## Detailed Description
pg_terminate_backend is a PostgreSQL built-in function that forcefully terminates backend processes. It provides more aggressive process termination compared to pg_cancel_backend by sending SIGTERM instead of SIGINT. The function offers two modes of operation:

1. **Signal-only Mode**: When timeout is 0, it just sends the SIGTERM signal and returns immediately
2. **Signal-and-wait Mode**: When timeout > 0, it sends SIGTERM and then waits for the process to actually terminate within the specified timeout

Key features include:
- **Permission Enforcement**: Uses pg_signal_backend() for consistent role-based access control
- **Input Validation**: Rejects negative timeout values with appropriate error messages
- **Flexible Operation**: Supports both fire-and-forget and wait-for-completion semantics
- **Comprehensive Error Reporting**: Provides detailed permission-denied messages

The function is typically used for forceful termination of problematic or unresponsive backend processes where pg_cancel_backend is insufficient.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument (PG_GETARG_INT32(0)): Process ID of the backend to terminate
  - Second argument (PG_GETARG_INT64(1)): Timeout in milliseconds (0 = no waiting, >0 = wait for termination)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_signal_backend](pg_signal_backend.md) (with SIGTERM signal)
  - [pg_wait_until_termination](pg_wait_until_termination.md) (when timeout > 0)
  - PG_GETARG_INT32
  - PG_GETARG_INT64
  - PG_RETURN_BOOL
  - ereport
- Called from (representative examples):
  - SQL queries (user-callable function)
  - Database administration tools

## Notes and Other Information
- Returns boolean: true if termination signal was successfully sent (and process terminated within timeout if applicable), false otherwise
- Sends SIGTERM signal, which is stronger than SIGINT used by pg_cancel_backend
- Validates that timeout is non-negative, raising ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE for negative values
- When timeout is 0, returns immediately after signaling - does not wait for actual termination
- When timeout > 0, uses pg_wait_until_termination() to confirm the process actually ends
- Permission errors are raised as ERROR level, aborting the current transaction
- Provides separate error messages for superuser privilege issues vs general role membership issues
- Part of PostgreSQL's process management infrastructure for administrative operations
- More forceful than query cancellation - designed for terminating unresponsive or problematic backends