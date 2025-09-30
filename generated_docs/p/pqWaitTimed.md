# pqWaitTimed

## Location
[src/interfaces/libpq/fe-misc.c:1020-1042](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L1020-L1042)

## Overview
pqWaitTimed is a PostgreSQL libpq function that waits for socket readiness with a specified timeout limit.

## Definition

```c
int
pqWaitTimed(int forRead, int forWrite, PGconn *conn, pg_usec_time_t end_time)
```
## Detailed Description
pqWaitTimed provides timed socket waiting functionality for PostgreSQL client connections. It waits for the connection socket to become ready for reading or writing operations, but will not wait past the specified end time. The function delegates the actual socket checking to pqSocketCheck and handles the return value interpretation. It returns different values to indicate success (socket ready), timeout, or failure conditions.

## Parameters / Member Variables
- : Integer flag indicating whether to wait for read readiness (non-zero means wait for read)
- : Integer flag indicating whether to wait for write readiness (non-zero means wait for write)
- : Pointer to the PGconn connection structure representing the database connection
- : Timeout specified as microseconds since Unix epoch (pg_usec_time_t). Use -1 for infinite timeout, 0 for immediate return

## Dependencies
- Functions called/Symbols referenced:
  - [pqSocketCheck](pqSocketCheck.md)
  - pg_usec_time_t (type)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
- Called from (representative examples):
  - [pqConnectDBComplete](pqConnectDBComplete.md)
  - [pqWait](pqWait.md)

## Notes and Other Information
- Returns -1 on failure (error message set), 0 if socket is ready, 1 if timeout occurred
- Timeout of -1 means infinite wait, 0 means immediate return (no blocking)
- On timeout, appends "timeout expired" error message to the connection
- The end_time parameter uses microsecond precision for fine-grained timeout control
- File location: src/interfaces/libpq/fe-misc.c:1020-1042

## Simplified Source

```c
int pqWaitTimed(int forRead, int forWrite, PGconn *conn, pg_usec_time_t end_time)
{
    // Check socket readiness with timeout
    int result = pqSocketCheck(conn, forRead, forWrite, end_time);

    // Handle socket check failure
    if (result < 0)
        return -1;  // Error already set in connection

    // Handle timeout case
    if (result == 0) {
        libpq_append_conn_error(conn, "timeout expired");
        return 1;   // Timeout occurred
    }

    return 0;       // Socket is ready
}
```