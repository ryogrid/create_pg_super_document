# CopyStreamPoll

## Location
[src/bin/pg_basebackup/receivelog.c:870-931](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/receivelog.c#L870-L931)

## Overview
CopyStreamPoll is a utility function that waits until data becomes available for reading on a PostgreSQL connection socket, with optional timeout and signal handling for graceful termination.

## Definition

```c
struct timeval timeout;
```
## Detailed Description
This function implements a blocking wait mechanism using select() to monitor file descriptors for read availability. It's specifically designed for streaming replication scenarios where the client needs to wait for incoming CopyData messages from the server while being responsive to timeout conditions and external termination signals. The function monitors both the PostgreSQL connection socket and an optional stop socket that can be used to interrupt the wait operation. It handles various edge cases including signal interruption (EINTR) and invalid socket conditions.

## Parameters / Member Variables
- : PostgreSQL connection object from which to obtain the socket descriptor
- : Timeout value in milliseconds; negative values mean wait indefinitely, 0 means don't wait
- : Optional socket descriptor that can be used to interrupt the wait operation; use PGINVALID_SOCKET if not needed

## Dependencies
- Functions called/Symbols referenced:
  - [PQsocket](../P/PQsocket.md)
  - select
  - FD_ZERO, FD_SET, FD_ISSET (file descriptor set macros)
  - Max (macro for maximum value)
  - pg_log_error (logging function)
  - [PQerrorMessage](../P/PQerrorMessage.md)
- Called from (representative examples):
  - [CopyStreamReceive](CopyStreamReceive.md)

## Notes and Other Information
- Returns 1 if data is available for reading, 0 if timed out or interrupted, -1 on error
- Handles EINTR signal interruption gracefully by returning 0 rather than treating it as an error
- Uses fd_set and select() for portable socket monitoring across different platforms
- The stop_socket mechanism allows for clean shutdown of streaming operations
- Part of the pg_basebackup utility's streaming replication functionality

## Simplified Source

```c
/*
 * Wait until we can read a CopyData message,
 * or timeout, or occurrence of a signal or input on the stop_socket.
 * Returns 1 if data available, 0 if timeout/interrupted, -1 on error.
 */
static int
CopyStreamPoll(PGconn *conn, long timeout_ms, pgsocket stop_socket)
{
    fd_set input_mask;
    struct timeval timeout, *timeoutptr;
    int connsocket, maxfd, ret;

    // Get connection socket and validate
    connsocket = PQsocket(conn);
    if (connsocket < 0) {
        pg_log_error("invalid socket: %s", PQerrorMessage(conn));
        return -1;
    }

    // Setup file descriptor set for monitoring
    FD_ZERO(&input_mask);
    FD_SET(connsocket, &input_mask);
    maxfd = connsocket;

    // Add stop socket if provided
    if (stop_socket != PGINVALID_SOCKET) {
        FD_SET(stop_socket, &input_mask);
        maxfd = Max(maxfd, stop_socket);
    }

    // Setup timeout
    if (timeout_ms < 0) {
        timeoutptr = NULL;  // Wait indefinitely
    } else {
        timeout.tv_sec = timeout_ms / 1000L;
        timeout.tv_usec = (timeout_ms % 1000L) * 1000L;
        timeoutptr = &timeout;
    }

    // Poll for activity
    ret = select(maxfd + 1, &input_mask, NULL, NULL, timeoutptr);

    // Handle results
    if (ret < 0) {
        if (errno == EINTR)
            return 0;  // Signal received, not an error
        pg_log_error("select() failed: %m");
        return -1;
    }

    if (ret > 0 && FD_ISSET(connsocket, &input_mask))
        return 1;  // Data available on connection

    return 0;  // Timeout or stop_socket activity
}
```