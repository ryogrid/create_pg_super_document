# test_uniqviol

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:1921-2088](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L1921-L2088)

## Overview
Tests PostgreSQL pipeline behavior with mixed successful and error-producing queries using non-blocking I/O and prepared statements, specifically testing unique constraint violation handling.

## Definition
```c
static void test_uniqviol(PGconn *conn)
```

## Detailed Description
This sophisticated test function validates pipeline behavior in non-blocking mode when mixing successful queries with intentional unique constraint violations. The test implements a complex I/O pattern that:

1. **Setup**: Creates a test table with a primary key, begins a transaction, and prepares an INSERT statement
2. **Non-blocking Pipeline**: Switches to non-blocking mode and enters pipeline mode
3. **Mixed Query Stream**: Sends a stream of INSERT queries using `select()` for I/O multiplexing:
   - Most queries insert unique values (successful)
   - One query intentionally violates the primary key constraint (error)
4. **Socket Management**: Carefully manages socket fullness and reading/writing phases to avoid deadlocks
5. **Error Injection**: Strategically injects a uniqueness violation after switching to read mode
6. **Result Processing**: Uses `process_result()` helper function to validate and handle both successful and error results

The test demonstrates:
- Non-blocking pipeline I/O handling
- Proper error recovery in pipeline mode
- Socket buffer management and flow control
- Mixed success/error result processing

## Parameters / Member Variables
- `conn`: PostgreSQL connection object (`PGconn *`) configured for non-blocking pipeline operations

## Dependencies
- Functions called/Symbols referenced:
  - [PQsocket](../P/PQsocket.md) - Get connection socket descriptor
  - [PQsetnonblocking](../P/PQsetnonblocking.md) - Enable non-blocking mode
  - [PQexec](../P/PQexec.md) - Execute immediate SQL commands
  - [PQprepare](../P/PQprepare.md) - Prepare a statement
  - [PQenterPipelineMode](../P/PQenterPipelineMode.md) - Enter pipeline mode
  - [PQisBusy](../P/PQisBusy.md) - Check if connection is busy
  - [PQgetResult](../P/PQgetResult.md) - Retrieve query results
  - [PQconsumeInput](../P/PQconsumeInput.md) - Read available input from server
  - [PQsendQueryPrepared](../P/PQsendQueryPrepared.md) - Execute prepared statements
  - [PQsendFlushRequest](../P/PQsendFlushRequest.md) - Send flush request
  - [PQflush](../P/PQflush.md) - Flush outgoing data
  - [process_result](../p/process_result.md) - Helper function to process individual results
  - select - System call for I/O multiplexing
  - FD_ZERO, FD_SET, FD_ISSET - File descriptor set macros
  - [PQresultStatus](../P/PQresultStatus.md) - Get result status
  - [PQerrorMessage](../P/PQerrorMessage.md) - Get error message
  - PGRES_COMMAND_OK - [Command](../C/Command.md) executed successfully
  - MAXINT8LEN - Maximum length of int8 string representation
  - EINTR - Interrupted system call error code
- Called from (representative examples):
  - [main](../m/main.md) - Main test driver function

## Notes and Other Information
- This is an advanced test for non-blocking pipeline I/O with error handling
- Creates and uses a test table `ppln_uniqviol` with a bigint primary key
- Uses prepared statements for efficient repeated execution
- Implements sophisticated flow control to prevent deadlocks in non-blocking mode
- Intentionally triggers a unique constraint violation to test error handling
- Uses `select()` system call for proper I/O multiplexing between reading and writing
- The test manages socket buffer fullness and switches between read/write phases accordingly
- Part of the libpq_pipeline test module located in `src/test/modules/libpq_pipeline/`
- Demonstrates real-world patterns for high-performance database applications
- The error is injected strategically after the connection has switched to read mode
- Validates that exactly one error occurs during the entire operation
- Shows proper handling of mixed successful and failed operations in a single pipeline
- Uses parameterized queries with bigint parameters for the INSERT operations
- Implements proper cleanup and error detection throughout the complex I/O loop

## Simplified Source

```c
static void test_uniqviol(PGconn *conn) {
    int sock = PQsocket(conn);
    PGresult *res;
    Oid paramTypes[2] = {INT8OID, INT8OID};
    const char *paramValues[2];
    char paramValue0[MAXINT8LEN];
    char paramValue1[MAXINT8LEN];
    int ctr = 0;
    int numsent = 0;
    int results = 0;
    bool read_done = false;
    bool write_done = false;
    bool error_sent = false;
    bool got_error = false;
    int switched = 0;
    int socketful = 0;
    fd_set in_fds;
    fd_set out_fds;

    fprintf(stderr, "uniqviol ...");

    PQsetnonblocking(conn, 1);

    paramValues[0] = paramValue0;
    paramValues[1] = paramValue1;
    sprintf(paramValue1, "42");

    // Setup table and transaction
    res = PQexec(conn, "drop table if exists ppln_uniqviol;"
                      "create table ppln_uniqviol(id bigint primary key, idata bigint)");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        pg_fatal("failed to create table");

    res = PQexec(conn, "begin");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        pg_fatal("failed to begin transaction");

    // Prepare INSERT statement
    res = PQprepare(conn, "insertion",
                   "insert into ppln_uniqviol values ($1, $2) returning id",
                   2, paramTypes);
    if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK)
        pg_fatal("failed to prepare query");

    if (PQenterPipelineMode(conn) != 1)
        pg_fatal("failed to enter pipeline mode");

    // Main I/O loop: mix reading and writing with non-blocking sockets
    while (!read_done) {
        // Read available results first to avoid deadlocks
        while (PQisBusy(conn) == 0) {
            bool new_error;

            if (results >= numsent) {
                if (write_done)
                    read_done = true;
                break;
            }

            res = PQgetResult(conn);
            new_error = process_result(conn, res, results, numsent);
            if (new_error && got_error)
                pg_fatal("got two errors");
            got_error |= new_error;
            if (results++ >= numsent - 1) {
                if (write_done)
                    read_done = true;
                break;
            }
        }

        if (read_done)
            break;

        // Use select() to multiplex I/O
        FD_ZERO(&out_fds);
        FD_SET(sock, &out_fds);
        FD_ZERO(&in_fds);
        FD_SET(sock, &in_fds);

        if (select(sock + 1, &in_fds, write_done ? NULL : &out_fds, NULL, NULL) == -1) {
            if (errno == EINTR)
                continue;
            pg_fatal("select() failed: %m");
        }

        if (FD_ISSET(sock, &in_fds) && PQconsumeInput(conn) == 0)
            pg_fatal("PQconsumeInput failed");

        // Send queries when socket is writable
        if (!write_done && FD_ISSET(sock, &out_fds)) {
            for (;;) {
                int flush;

                // Inject uniqueness violation once after switching to read mode
                if (switched >= 1 && !error_sent && ctr % socketful >= socketful / 2) {
                    sprintf(paramValue0, "%d", numsent / 2);
                    fprintf(stderr, "E");
                    error_sent = true;
                } else {
                    fprintf(stderr, ".");
                    sprintf(paramValue0, "%d", ctr++);
                }

                if (PQsendQueryPrepared(conn, "insertion", 2, paramValues, NULL, NULL, 0) != 1)
                    pg_fatal("failed to execute prepared query");
                numsent++;

                // Check if done writing
                if (socketful != 0 && numsent % socketful == 42 && error_sent) {
                    if (PQsendFlushRequest(conn) != 1)
                        pg_fatal("failed to send flush request");
                    write_done = true;
                    fprintf(stderr, "\ndone writing\n");
                    PQflush(conn);
                    break;
                }

                // Check if socket buffer is full
                flush = PQflush(conn);
                if (flush == -1)
                    pg_fatal("failed to flush");
                if (flush == 1) {
                    if (socketful == 0)
                        socketful = numsent;
                    fprintf(stderr, "\nswitch to reading\n");
                    switched++;
                    break;
                }
            }
        }
    }

    if (!got_error)
        pg_fatal("did not get expected error");

    fprintf(stderr, "ok\n");
}
```