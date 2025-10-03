# PQisBusy

## Location
[src/interfaces/libpq/fe-exec.c:2031-2061](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2031-L2061)

## Overview
Public libpq function that determines whether PQgetResult would block waiting for input from the server.

## Definition

```c
int
PQisBusy(PGconn *conn)
```
## Detailed Description
PQisBusy is a non-blocking function that checks if there are complete messages available from the server that can be processed immediately. It returns true if a call to PQgetResult would need to wait for more data from the server, and false if PQgetResult can return a result immediately.

The function first attempts to parse any available buffered data by calling parseInput, which processes messages that have already been received. After parsing, it checks the connection's asynchronous status. The function returns true only when the connection is in PGASYNC_BUSY state (indicating a query is in progress but no complete result is available) and the connection is still healthy (not CONNECTION_BAD).

This function is essential for implementing non-blocking query processing patterns in applications that need to avoid blocking on database operations.

## Parameters / Member Variables
- `*conn`: Pointer to the PGconn structure representing the database connection to check
## Dependencies
- Functions called/Symbols referenced:
  - [parseInput](../p/parseInput.md)
  - CONNECTION_BAD
  - PGASYNC_BUSY
- Called from (representative examples):
  - [libpqrcv_PQgetResult](../l/libpqrcv_PQgetResult.md)
  - [advanceConnectionState](../a/advanceConnectionState.md)
  - [wait_on_slots](../w/wait_on_slots.md)
  - [libpqsrv_get_result](../l/libpqsrv_get_result.md)
  - [try_complete_step](../t/try_complete_step.md)

## Notes and Other Information
- Returns false if conn is NULL for safety
- This function does not perform any I/O operations - it only checks existing state
- The function specifically ignores write_failed status, focusing only on read availability
- Part of the public libpq API for asynchronous query processing
- Essential for event-driven and non-blocking database applications
- Returns 0 (false) if results are available, 1 (true) if PQgetResult would block

## Simplified Source

```c
int PQisBusy(PGconn *conn) {
    // Handle null connection
    if (!conn)
        return false;

    // Parse any available buffered data
    parseInput(conn);

    // Return true if query is busy and connection is healthy
    // PQgetResult would block only in PGASYNC_BUSY state with good connection
    return conn->asyncStatus == PGASYNC_BUSY && conn->status != CONNECTION_BAD;
}
```