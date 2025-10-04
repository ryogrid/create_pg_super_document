# test_cancel

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:245-408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L245-L408)

## Overview
Comprehensive test function that validates all PostgreSQL query cancellation mechanisms including PQcancel, PQrequestCancel, PQcancelBlocking, and asynchronous polling-based cancellation.

## Definition

```c
struct timeval tv;
```
## Detailed Description
The  function is a comprehensive test suite that exercises all the different query cancellation mechanisms provided by libpq. It tests both synchronous and asynchronous cancellation methods, including the traditional PQcancel() and PQrequestCancel() functions, as well as the newer blocking and polling-based cancellation APIs.

The function performs the following test sequence:
1. Tests PQcancel() with a reusable PGcancel object
2. Tests PQrequestCancel() for simple cancellation
3. Tests PQcancelBlocking() for synchronous blocking cancellation
4. Tests asynchronous cancellation using PQcancelCreate(), PQcancelStart(), and PQcancelPoll()
5. Tests PQcancelReset() to verify cancel connection reusability

Each test involves sending a long-running cancellable query using a separate monitoring connection, then attempting to cancel it using different methods, and finally confirming the cancellation was successful.

## Parameters / Member Variables
- : The main database connection on which queries will be executed and cancelled

## Dependencies
- Functions called/Symbols referenced:
  - [PQsetnonblocking](../P/PQsetnonblocking.md)
  - [copy_connection](../c/copy_connection.md)
  - send_cancellable_query
  - confirm_query_canceled
  - [PQgetCancel](../P/PQgetCancel.md)
  - [PQcancel](../P/PQcancel.md)
  - [PQfreeCancel](../P/PQfreeCancel.md)
  - [PQrequestCancel](../P/PQrequestCancel.md)
  - [PQcancelCreate](../P/PQcancelCreate.md)
  - [PQcancelBlocking](../P/PQcancelBlocking.md)
  - [PQcancelStart](../P/PQcancelStart.md)
  - [PQcancelPoll](../P/PQcancelPoll.md)
  - [PQcancelSocket](../P/PQcancelSocket.md)
  - [PQcancelStatus](../P/PQcancelStatus.md)
  - [PQcancelReset](../P/PQcancelReset.md)
  - [PQcancelFinish](../P/PQcancelFinish.md)
  - [PQcancelErrorMessage](../P/PQcancelErrorMessage.md)
  - [PQstatus](../P/PQstatus.md)
  - CONNECTION_OK
  - PGRES_POLLING_OK
  - PGRES_POLLING_READING
  - PGRES_POLLING_WRITING
  - select
  - pg_debug
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- This is a static function within the libpq_pipeline test module
- The function sets the main connection to non-blocking mode for testing
- Uses a separate monitor connection to track query execution status
- Implements proper polling loops with select() for asynchronous cancellation testing
- Tests both one-time and reusable cancellation objects
- Includes comprehensive error handling and status checking
- Located in src/test/modules/libpq_pipeline/libpq_pipeline.c at lines 245-408
- Part of the PostgreSQL test suite for validating cancellation functionality
- Uses file descriptor-based polling with proper timeout handling (3 second timeout)

## Simplified Source

```c
static void test_cancel(PGconn *conn) {
    PGcancel *cancel;
    PGcancelConn *cancelConn;
    PGconn *monitorConn;
    char errorbuf[256];

    fprintf(stderr, "test cancellations... ");

    // Set connection to non-blocking mode
    if (PQsetnonblocking(conn, 1) != 0)
        pg_fatal("failed to set nonblocking mode: %s", PQerrorMessage(conn));

    // Create monitor connection to track query state
    monitorConn = copy_connection(conn);

    // Test 1: PQcancel with reusable cancel object
    send_cancellable_query(conn, monitorConn);
    cancel = PQgetCancel(conn);
    if (!PQcancel(cancel, errorbuf, sizeof(errorbuf)))
        pg_fatal("failed to run PQcancel: %s", errorbuf);
    confirm_query_canceled(conn);

    // Test 2: Reuse PGcancel object
    send_cancellable_query(conn, monitorConn);
    if (!PQcancel(cancel, errorbuf, sizeof(errorbuf)))
        pg_fatal("failed to run PQcancel: %s", errorbuf);
    confirm_query_canceled(conn);
    PQfreeCancel(cancel);

    // Test 3: PQrequestCancel
    send_cancellable_query(conn, monitorConn);
    if (!PQrequestCancel(conn))
        pg_fatal("failed to run PQrequestCancel: %s", PQerrorMessage(conn));
    confirm_query_canceled(conn);

    // Test 4: PQcancelBlocking (synchronous)
    send_cancellable_query(conn, monitorConn);
    cancelConn = PQcancelCreate(conn);
    if (!PQcancelBlocking(cancelConn))
        pg_fatal("failed to run PQcancelBlocking: %s", PQcancelErrorMessage(cancelConn));
    confirm_query_canceled(conn);
    PQcancelFinish(cancelConn);

    // Test 5: Asynchronous cancellation with polling
    send_cancellable_query(conn, monitorConn);
    cancelConn = PQcancelCreate(conn);
    if (!PQcancelStart(cancelConn))
        pg_fatal("bad cancel connection: %s", PQcancelErrorMessage(cancelConn));

    // Poll until cancellation completes
    while (PQcancelPoll(cancelConn) != PGRES_POLLING_OK) {
        // Handle polling state with select() - details omitted for brevity
    }
    confirm_query_canceled(conn);

    // Test 6: PQcancelReset for connection reuse
    PQcancelReset(cancelConn);
    send_cancellable_query(conn, monitorConn);

    // Repeat polling cancellation to test reset functionality
    if (!PQcancelStart(cancelConn))
        pg_fatal("bad cancel connection: %s", PQcancelErrorMessage(cancelConn));
    while (PQcancelPoll(cancelConn) != PGRES_POLLING_OK) {
        // Handle polling state - details omitted
    }
    confirm_query_canceled(conn);

    PQcancelFinish(cancelConn);
    fprintf(stderr, "ok\n");
}
```