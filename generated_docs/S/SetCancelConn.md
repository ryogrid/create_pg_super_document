# SetCancelConn

## Location
src/fe_utils/cancel.c: 77 - 106

## Overview
SetCancelConn sets the global cancel connection object to point to the current database connection, enabling the ability to cancel queries running on that connection.

## Definition


## Detailed Description
SetCancelConn is a thread-safe function that manages the global cancelConn variable, which holds a PGcancel object used for canceling database queries. The function safely replaces any existing cancel connection with a new one derived from the provided database connection.

The function uses critical sections on Windows to ensure thread safety during the connection replacement operation. It follows a careful sequence: first nullifying the global pointer, then freeing the old cancel object, and finally setting the new cancel object. This prevents race conditions with signal handlers that might attempt to use the cancel connection simultaneously.

## Parameters / Member Variables
- : A pointer to the PostgreSQL database connection (PGconn) from which to create the cancel connection object

## Dependencies
- Functions called/Symbols referenced:
  - PQfreeCancel
  - PQgetCancel
  - PGcancel (type)
- Called from (representative examples):
  - runInitSteps (pgbench)
  - PSQLexec (psql)
  - PSQLexecWatch (psql)
  - HandleCopyResult (psql)
  - SendQuery (psql)
  - consumeQueryResult (parallel_slot)

## Notes and Other Information
- Uses Windows-specific critical sections (EnterCriticalSection/LeaveCriticalSection) for thread safety on WIN32 platforms
- Carefully sets cancelConn to NULL before freeing the old connection to prevent race conditions with signal handlers
- Part of the frontend utilities cancel mechanism, typically used by PostgreSQL client tools to enable query cancellation via Ctrl+C
- The global cancelConn variable is used by signal handlers like handle_sigint to cancel running queries