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

## Simplified Source

```c
static void StreamLogicalLog(void) {
    PGresult *res;
    char *copybuf = NULL;
    TimestampTz last_status = -1;
    PQExpBuffer query;
    XLogRecPtr cur_record_lsn;

    // Initialize LSN tracking
    output_written_lsn = InvalidXLogRecPtr;
    output_fsync_lsn = InvalidXLogRecPtr;

    // Establish replication connection
    if (!conn) {
        conn = GetConnection();
    }
    if (!conn) {
        return;
    }

    // Build and execute START_REPLICATION command
    query = createPQExpBuffer();
    appendPQExpBuffer(query, "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X",
                     replication_slot, LSN_FORMAT_ARGS(startpos));

    // Add any configured options
    if (noptions) {
        appendPQExpBufferStr(query, " (");
        for (int i = 0; i < noptions; i++) {
            if (i > 0) appendPQExpBufferStr(query, ", ");
            appendPQExpBuffer(query, "\"%s\"", options[i * 2]);
            if (options[i * 2 + 1] != NULL) {
                appendPQExpBuffer(query, " '%s'", options[i * 2 + 1]);
            }
        }
        appendPQExpBufferChar(query, ')');
    }

    res = PQexec(conn, query->data);
    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        pg_log_error("could not send replication command \"%s\": %s",
                    query->data, PQresultErrorMessage(res));
        goto cleanup;
    }

    if (verbose) {
        pg_log_info("streaming initiated");
    }

    // Main streaming loop
    while (!time_to_abort) {
        TimestampTz now = feGetCurrentTimestamp();

        // Handle periodic operations (fsync, keepalives)
        handle_periodic_operations(now, &last_status);

        // Open output file if needed
        if (outfd == -1) {
            open_output_file();
        }

        // Get next message from stream
        int r = PQgetCopyData(conn, &copybuf, 1);

        if (r == 0) {
            // No data available, wait with timeout
            wait_for_data_with_timeout(conn, now, &last_status);
            continue;
        }

        if (r == -1) {
            break; // End of stream
        }

        if (r == -2) {
            pg_log_error("could not read COPY data: %s", PQerrorMessage(conn));
            goto cleanup;
        }

        // Process message based on type
        if (copybuf[0] == 'k') {
            // Keepalive message
            if (!process_keepalive_message(copybuf, r, conn, &now, &last_status)) {
                goto cleanup;
            }
        } else if (copybuf[0] == 'w') {
            // WAL data message
            if (!process_wal_data_message(copybuf, r, &cur_record_lsn, &now)) {
                goto cleanup;
            }
        } else {
            pg_log_error("unrecognized streaming header: \"%c\"", copybuf[0]);
            goto cleanup;
        }

        // Check if we've reached the end position
        if (endpos != InvalidXLogRecPtr && cur_record_lsn >= endpos) {
            flushAndSendFeedback(conn, &now);
            time_to_abort = true;
            break;
        }

        if (copybuf) {
            PQfreemem(copybuf);
            copybuf = NULL;
        }
    }

    // Handle clean termination
    if (time_to_abort) {
        prepareToTerminate(conn, endpos, stop_reason, cur_record_lsn);
    }

cleanup:
    // Resource cleanup
    if (copybuf) PQfreemem(copybuf);
    if (outfd != -1 && strcmp(outfile, "-") != 0) {
        OutputFsync(feGetCurrentTimestamp());
        close(outfd);
    }
    destroyPQExpBuffer(query);
    PQfinish(conn);
    conn = NULL;
    outfd = -1;
}
```