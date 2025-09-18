# pg_cancel_backend

## Location
[src/backend/storage/ipc/signalfuncs.c:122-147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/signalfuncs.c#L122-L147)

## Overview
SQL-callable function that cancels a running query in a PostgreSQL backend process by sending a SIGINT signal, with appropriate permission checking.

## Definition


## Detailed Description
pg_cancel_backend is a PostgreSQL built-in function that allows users to cancel queries running in other backend processes. It serves as a wrapper around pg_signal_backend(), specifically sending a SIGINT signal to interrupt query execution. The function enforces strict access control:

1. **Permission Validation**: Uses the underlying pg_signal_backend() permission system
2. **Role-based Access**: Users can only cancel queries from processes they have role membership privileges for
3. **Superuser Protection**: Only superusers can cancel queries from superuser-owned processes
4. **Error Reporting**: Converts internal error codes to user-friendly SQL errors with detailed messages

The function is typically invoked through SQL as `SELECT pg_cancel_backend(pid)` and is commonly used in database administration scenarios to stop runaway or problematic queries.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument (PG_GETARG_INT32(0)): Process ID of the backend whose query should be canceled

## Dependencies
- Functions called/Symbols referenced:
  - [pg_signal_backend](pg_signal_backend.md) (with SIGINT signal)
  - PG_GETARG_INT32
  - PG_RETURN_BOOL
  - ereport
- Called from (representative examples):
  - SQL queries (user-callable function)
  - Database administration tools

## Notes and Other Information
- Returns boolean: true if cancellation signal was successfully sent, false otherwise
- Sends SIGINT signal specifically, which triggers query cancellation in PostgreSQL backends
- Permission errors are raised as ERROR level (which abort the transaction) rather than WARNING
- Provides detailed error messages distinguishing between superuser permission issues and general role membership issues
- Part of PostgreSQL's signal handling infrastructure for process management
- Does not guarantee the query will actually be canceled - depends on the target backend's responsiveness to SIGINT