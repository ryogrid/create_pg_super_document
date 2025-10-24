# SetCancelConn

## Location
[src/fe_utils/cancel.c:77-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/cancel.c#L77-L106)

## Overview
SetCancelConn sets the global cancel connection object to point to the current database connection, enabling the ability to cancel queries running on that connection.

## Definition

```c
void
SetCancelConn(PGconn *conn)
```
## Detailed Description
SetCancelConn is a thread-safe function that manages the global cancelConn variable, which holds a PGcancel object used for canceling database queries. The function safely replaces any existing cancel connection with a new one derived from the provided database connection.

The function uses critical sections on Windows to ensure thread safety during the connection replacement operation. It follows a careful sequence: first nullifying the global pointer, then freeing the old cancel object, and finally setting the new cancel object. This prevents race conditions with signal handlers that might attempt to use the cancel connection simultaneously.

## Parameters / Member Variables
- `*conn`: A pointer to the PostgreSQL database connection (PGconn) from which to create the cancel connection object
## Dependencies
- Functions called/Symbols referenced:
  - [PQfreeCancel](../P/PQfreeCancel.md)
  - [PQgetCancel](../P/PQgetCancel.md)
  - [PGcancel](../P/PGcancel.md) (type)
- Called from (representative examples):
  - [runInitSteps](../r/runInitSteps.md) (pgbench)
  - [PSQLexec](../P/PSQLexec.md) (psql)
  - [PSQLexecWatch](../P/PSQLexecWatch.md) (psql)
  - [HandleCopyResult](../H/HandleCopyResult.md) (psql)
  - [SendQuery](SendQuery.md) (psql)
  - [consumeQueryResult](../c/consumeQueryResult.md) (parallel_slot)

## Notes and Other Information
- Uses Windows-specific critical sections (EnterCriticalSection/LeaveCriticalSection) for thread safety on WIN32 platforms
- Carefully sets cancelConn to NULL before freeing the old connection to prevent race conditions with signal handlers
- Part of the frontend utilities cancel mechanism, typically used by PostgreSQL client tools to enable query cancellation via Ctrl+C
- The global cancelConn variable is used by signal handlers like handle_sigint to cancel running queries

## Simplified Source

```c
void SetCancelConn(PGconn *conn) {
    PGcancel *oldCancelConn;

#ifdef WIN32
    EnterCriticalSection(&cancelConnLock);
#endif

    // Save old connection and temporarily set to NULL for thread safety
    oldCancelConn = cancelConn;
    cancelConn = NULL;

    // Free old cancel connection if it exists
    if (oldCancelConn != NULL)
        PQfreeCancel(oldCancelConn);

    // Set new cancel connection
    cancelConn = PQgetCancel(conn);

#ifdef WIN32
    LeaveCriticalSection(&cancelConnLock);
#endif
}
```