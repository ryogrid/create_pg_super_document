# pg_reload_conf

## Location
src/backend/storage/ipc/signalfuncs.c: 260 - 279

## Overview
SQL-callable function that triggers PostgreSQL configuration reload by sending a SIGHUP signal to the postmaster process.

## Definition
```c
Datum pg_reload_conf(PG_FUNCTION_ARGS)
```

## Detailed Description
pg_reload_conf is a PostgreSQL built-in function that initiates a configuration reload without requiring a database restart. It accomplishes this by sending a SIGHUP (hang-up) signal directly to the postmaster process, which is the standard Unix mechanism for telling a daemon to reload its configuration files.

Unlike the backend signaling functions (pg_cancel_backend, pg_terminate_backend), this function:
1. **Targets the Postmaster**: Sends signal to PostmasterPid rather than a backend process
2. **Uses SIGHUP**: Sends SIGHUP signal which conventionally means "reload configuration"
3. **Simple Implementation**: No complex permission checking beyond SQL GRANT system
4. **Direct Operation**: No intermediate helper functions - calls kill() directly

The function is commonly used by database administrators to apply configuration changes from postgresql.conf and other configuration files without downtime.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure (no specific arguments required for this function)

## Dependencies
- Functions called/Symbols referenced:
  - kill (system call)
  - PostmasterPid (global variable)
  - SIGHUP (signal constant)
  - ereport
  - PG_RETURN_BOOL
- Called from (representative examples):
  - SQL queries (user-callable function)
  - Database administration scripts
  - Configuration management tools

## Notes and Other Information
- Returns boolean: true if SIGHUP signal was successfully sent to postmaster, false if kill() failed
- Permission checking relies on PostgreSQL's standard GRANT system rather than custom role-based logic
- Uses WARNING level for errors (non-fatal) rather than ERROR level
- Targets the postmaster process specifically via PostmasterPid global variable
- Triggering configuration reload affects the entire PostgreSQL cluster, not just individual backends
- Some configuration parameters require a full restart and cannot be reloaded via SIGHUP
- The function returns immediately after signaling - it doesn't wait for configuration reload to complete
- Commonly used after editing postgresql.conf, pg_hba.conf, or other configuration files