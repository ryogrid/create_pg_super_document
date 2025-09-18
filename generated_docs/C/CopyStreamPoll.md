# CopyStreamPoll

## Location
[src/bin/pg_basebackup/receivelog.c:870-931](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/receivelog.c#L870-L931)

## Overview
CopyStreamPoll is a utility function that waits until data becomes available for reading on a PostgreSQL connection socket, with optional timeout and signal handling for graceful termination.

## Definition


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