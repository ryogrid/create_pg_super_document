# HandleCopyStream

## Location
[src/bin/pg_basebackup/receivelog.c:745-869](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/receivelog.c#L745-L869)

## Overview
Main event loop that handles the COPY stream protocol for PostgreSQL streaming replication, processing WAL data messages and keepalive messages while managing feedback and synchronization.

## Definition

```c
struct timeval timeout;
```
## Detailed Description
 implements the core message processing loop for PostgreSQL streaming replication after the START_REPLICATION command is issued. It handles the bidirectional COPY stream protocol, receiving WAL data and keepalive messages from the server while sending periodic feedback messages back. The function manages synchronous flushing when required, implements timeout-based feedback sending to prevent connection drops, and processes different message types ('w' for WAL data, 'k' for keepalives). It also handles stop conditions through callbacks and ensures proper cleanup on errors.

The function is central to the streaming replication protocol implementation and coordinates between data reception, processing, and feedback mechanisms.

## Parameters / Member Variables
- : PostgreSQL connection handle for the replication session
- : StreamCtl structure containing streaming parameters and configuration
- : Output parameter to receive the last WAL position processed when streaming ends

## Dependencies
- Functions called/Symbols referenced:
  - [CheckCopyStreamStop](../C/CheckCopyStreamStop.md)
  - [feGetCurrentTimestamp](../f/feGetCurrentTimestamp.md)
  - [sendFeedback](../s/sendFeedback.md)
  - [feTimestampDifferenceExceeds](../f/feTimestampDifferenceExceeds.md)
  - [CalculateCopyStreamSleeptime](../C/CalculateCopyStreamSleeptime.md)
  - [CopyStreamReceive](../C/CopyStreamReceive.md)
  - [HandleEndOfCopyStream](HandleEndOfCopyStream.md)
  - [ProcessKeepaliveMsg](../P/ProcessKeepaliveMsg.md)
  - [ProcessXLogDataMsg](../P/ProcessXLogDataMsg.md)
  - [GetLastWalMethodError](../G/GetLastWalMethodError.md)
  - [PQfreemem](../P/PQfreemem.md)
- Called from (representative examples):
  - [ReceiveXlogStream](../R/ReceiveXlogStream.md)

## Notes and Other Information
- Implements the main message processing loop for streaming replication
- Handles two primary message types: 'w' (WAL data) and 'k' (keepalive)
- Manages synchronous WAL flushing when stream->synchronous is enabled
- Sends periodic feedback messages based on standby_message_timeout to prevent server timeouts
- Uses non-blocking receive calls with calculated sleep times for efficiency
- Returns PGresult when COPY stream ends normally, NULL on error
- Critical component of PostgreSQL streaming replication protocol
- Static function used internally within the streaming replication infrastructure

## Simplified Source

```c
static PGresult *
HandleCopyStream(PGconn *conn, StreamCtl *stream, XLogRecPtr *stoppos)
{
    char       *copybuf = NULL;
    TimestampTz last_status = -1;
    XLogRecPtr  blockpos = stream->startpos;

    still_sending = true;

    while (1) {
        int         r;
        TimestampTz now;
        long        sleeptime;

        // Check if callback wants us to stop streaming
        if (!CheckCopyStreamStop(conn, stream, blockpos))
            goto error;

        now = feGetCurrentTimestamp();

        // Handle synchronous mode - flush WAL immediately when available
        if (stream->synchronous && lastFlushPosition < blockpos && walfile != NULL) {
            if (stream->walmethod->ops->sync(walfile) != 0)
                pg_fatal("could not fsync file \"%s\"", walfile->pathname);
            lastFlushPosition = blockpos;

            // Send feedback to inform server of latest position
            if (!sendFeedback(conn, blockpos, now, false))
                goto error;
            last_status = now;
        }

        // Send periodic status messages to prevent server timeout
        if (still_sending && stream->standby_message_timeout > 0 &&
            feTimestampDifferenceExceeds(last_status, now,
                                        stream->standby_message_timeout)) {
            if (!sendFeedback(conn, blockpos, now, false))
                goto error;
            last_status = now;
        }

        // Calculate optimal sleep time for next receive call
        sleeptime = CalculateCopyStreamSleeptime(now, stream->standby_message_timeout,
                                                last_status);

        // Receive data from COPY stream
        r = CopyStreamReceive(conn, sleeptime, stream->stop_socket, &copybuf);
        while (r != 0) {
            if (r == -1)
                goto error;
            if (r == -2) {
                // End of COPY stream
                PGresult *res = HandleEndOfCopyStream(conn, stream, copybuf,
                                                     blockpos, stoppos);
                if (res == NULL)
                    goto error;
                else
                    return res;
            }

            // Process different message types
            if (copybuf[0] == 'k') {
                // Keepalive message
                if (!ProcessKeepaliveMsg(conn, stream, copybuf, r, blockpos,
                                        &last_status))
                    goto error;
            } else if (copybuf[0] == 'w') {
                // WAL data message
                if (!ProcessXLogDataMsg(conn, stream, copybuf, r, &blockpos))
                    goto error;

                // Check if we should stop after processing WAL data
                if (!CheckCopyStreamStop(conn, stream, blockpos))
                    goto error;
            } else {
                pg_log_error("unrecognized streaming header: \"%c\"", copybuf[0]);
                goto error;
            }

            // Continue processing any additional available data
            r = CopyStreamReceive(conn, 0, stream->stop_socket, &copybuf);
        }
    }

error:
    PQfreemem(copybuf);
    return NULL;
}
```