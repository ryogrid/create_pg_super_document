# ResetCancelConn

## Location
src/fe_utils/cancel.c: 107 - 152

## Overview
ResetCancelConn safely clears and frees the global cancel connection object, removing the ability to cancel database queries.

## Definition


## Detailed Description
ResetCancelConn is a thread-safe function that cleans up the global cancelConn variable by freeing any existing PGcancel object and setting the pointer to NULL. This function is typically called when a database connection is closed or when cancellation capability is no longer needed.

Like SetCancelConn, this function uses critical sections on Windows platforms to ensure thread safety. It follows the same careful sequence of first nullifying the global pointer before freeing the cancel object to prevent race conditions with signal handlers that might attempt to access the cancel connection.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [PQfreeCancel](../P/PQfreeCancel.md)
  - PGcancel (type)
- Called from (representative examples):
  - [runInitSteps](../r/runInitSteps.md) (pgbench)
  - [do_connect](../d/do_connect.md) (psql)
  - [CheckConnection](../C/CheckConnection.md) (psql)
  - [PSQLexec](../P/PSQLexec.md) (psql)
  - [PSQLexecWatch](../P/PSQLexecWatch.md) (psql)
  - [HandleCopyResult](../H/HandleCopyResult.md) (psql)
  - [SendQuery](../S/SendQuery.md) (psql)
  - [consumeQueryResult](../c/consumeQueryResult.md) (parallel_slot)

## Notes and Other Information
- Uses Windows-specific critical sections for thread safety on WIN32 platforms
- Sets cancelConn to NULL before freeing to prevent race conditions with signal handlers
- Commonly called in cleanup scenarios, connection failures, or when switching database connections
- Safe to call even when cancelConn is already NULL - includes null check before freeing
- Part of the frontend utilities cancel mechanism used by PostgreSQL client tools