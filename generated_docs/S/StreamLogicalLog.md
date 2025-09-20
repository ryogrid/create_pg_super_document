# StreamLogicalLog

## Location
[src/bin/pg_basebackup/pg_recvlogical.c:213-673](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_recvlogical.c#L213-L673)

## Overview
StreamLogicalLog is the core function that implements the logical replication streaming protocol in pg_recvlogical, handling the complete lifecycle of receiving and writing logical WAL data from a PostgreSQL server.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
This function implements the main logical replication streaming loop for the pg_recvlogical utility. It establishes a replication connection to the PostgreSQL server, initiates logical replication from a specified slot, and continuously processes incoming WAL data messages until completion or termination.

Key responsibilities include:
1. **Connection Management**: Establishes replication connection using GetConnection()
2. **Replication Initiation**: Sends START_REPLICATION SLOT command with options
3. **Message Processing**: Handles different types of streaming messages:
   - 'w' messages: XLogData containing actual logical replication data
   - 'k' messages: Keepalive messages requiring optional feedback
4. **Output Management**: Opens, writes to, and manages output files with proper fsync
5. **Flow Control**: Sends periodic status updates and handles server feedback requests
6. **Timeout Handling**: Uses select() to handle timeouts for keepalives and fsync
7. **Graceful Termination**: Handles clean shutdown when reaching end positions
8. **Error Recovery**: Comprehensive error handling for all failure scenarios

The function operates in a continuous loop until reaching a specified end position, encountering an error, or receiving a termination signal. It manages both timing-based operations (periodic fsync, keepalive messages) and data-driven operations (processing received WAL records).

## Parameters / Member Variables
None - this function operates on global variables including:
- : PostgreSQL connection handle
- : Name of the logical replication slot
- : Starting WAL position for replication
- : Optional ending WAL position
- , : Output file name and descriptor
- Various timing and configuration globals

## Dependencies
- Functions called/Symbols referenced:
  - [GetConnection](../G/GetConnection.md), OutputFsync, sendFeedback, flushAndSendFeedback
  - PostgreSQL libpq functions: PQexec, PQgetCopyData, PQputCopyData, etc.
  - System calls: open, write, close, fstat, select
  - Utility functions: feGetCurrentTimestamp, feTimestampDifferenceExceeds
  - Logging functions: pg_log_info, pg_log_error
- Called from (representative examples):
  - [main](../m/main.md) (in pg_recvlogical.c:996) as the primary streaming function

## Notes and Other Information
- Static function serving as the core of pg_recvlogical functionality
- Implements PostgreSQL's logical replication protocol over a COPY_BOTH connection
- Handles both synchronous and asynchronous I/O with proper timeout management
- Provides comprehensive error handling and cleanup for robust operation
- Supports output to files, stdout, or other file descriptors
- Manages WAL position tracking for accurate progress reporting
- Critical for logical replication clients that need reliable WAL data streaming
- Contains complex state management for connection lifecycle and output file handling
- Uses select()-based event loop for efficient I/O multiplexing
- Implements proper resource cleanup even in error conditions