# ReceiveXlogStream

## Location
[src/bin/pg_basebackup/receivelog.c:453-698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/receivelog.c#L453-L698)

## Overview
Main function for receiving and processing a PostgreSQL WAL (Write-Ahead Log) stream from a server, handling timeline transitions and continuous streaming until a stop condition is met.

## Definition

```c
bool
ReceiveXlogStream(PGconn *conn, StreamCtl *stream)
```
## Detailed Description
 is the core function that orchestrates PostgreSQL streaming replication. It establishes and maintains a continuous WAL stream from the primary server, handling the complete lifecycle of replication including server version validation, system identifier verification, timeline history management, and automatic timeline transitions. The function runs in a loop, continuously streaming WAL data until explicitly stopped by a callback or server shutdown. It manages both physical and logical replication scenarios and handles various edge cases like timeline switches and partial WAL records at timeline boundaries.

The function supports both synchronous and asynchronous replication modes, handles replication slots for reliable delivery, and implements proper error handling and cleanup procedures.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle for the replication session
- `*stream`: StreamCtl structure containing all streaming parameters and callbacks including start position, timeline, stop conditions, and output methods
## Dependencies
- Functions called/Symbols referenced:
  - [CheckServerVersionForStreaming](../C/CheckServerVersionForStreaming.md)
  - [RunIdentifySystem](RunIdentifySystem.md)
  - [existsTimeLineHistoryFile](../e/existsTimeLineHistoryFile.md)
  - [writeTimeLineHistoryFile](../w/writeTimeLineHistoryFile.md)
  - [HandleCopyStream](../H/HandleCopyStream.md)
  - [ReadEndOfStreamingResult](ReadEndOfStreamingResult.md)
  - [PQexec](../P/PQexec.md)
  - [PQgetResult](../P/PQgetResult.md)
  - [pg_free](../p/pg_free.md)
  - XLogSegmentOffset
- Called from (representative examples):
  - [LogStreamerMain](../L/LogStreamerMain.md)
  - [StreamLog](../S/StreamLog.md)

## Notes and Other Information
- Requires WAL start position to be at a log segment boundary
- Automatically fetches missing timeline history files
- Supports replication slots for guaranteed WAL retention
- Handles timeline transitions by parsing server responses and restarting streaming on new timelines
- Implements flush position reporting for synchronous replication eligibility
- Validates system identifier and timeline consistency when specified
- Uses callback-based architecture for flexible stop conditions and data processing
- Critical component for pg_basebackup, pg_receivewal, and other replication tools

## Simplified Source

```c
bool
ReceiveXlogStream(PGconn *conn, StreamCtl *stream)
{
    char query[128];
    char slotcmd[128];
    PGresult *res;
    XLogRecPtr stoppos;

    // Check server version compatibility
    if (!CheckServerVersionForStreaming(conn))
        return false;

    // Configure replication slot and flush position reporting
    if (stream->replication_slot != NULL) {
        reportFlushPosition = true;
        sprintf(slotcmd, "SLOT \"%s\" ", stream->replication_slot);
    } else {
        reportFlushPosition = stream->synchronous;
        slotcmd[0] = 0;
    }

    // Validate system identifier and timeline if specified
    if (stream->sysidentifier != NULL) {
        char *sysidentifier = NULL;
        TimeLineID servertli;

        if (!RunIdentifySystem(conn, &sysidentifier, &servertli, NULL, NULL)) {
            pg_free(sysidentifier);
            return false;
        }

        if (strcmp(stream->sysidentifier, sysidentifier) != 0) {
            pg_log_error("system identifier mismatch");
            pg_free(sysidentifier);
            return false;
        }
        pg_free(sysidentifier);

        if (stream->timeline > servertli) {
            pg_log_error("starting timeline %u not present in server", stream->timeline);
            return false;
        }
    }

    lastFlushPosition = stream->startpos;

    // Main streaming loop - handles timeline transitions
    while (1) {
        // Fetch timeline history file if missing
        if (!existsTimeLineHistoryFile(stream)) {
            snprintf(query, sizeof(query), "TIMELINE_HISTORY %u", stream->timeline);
            res = PQexec(conn, query);
            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                pg_log_error("could not send TIMELINE_HISTORY command");
                PQclear(res);
                return false;
            }

            // Write history file to disk
            if (PQnfields(res) == 2 && PQntuples(res) == 1) {
                writeTimeLineHistoryFile(stream,
                                        PQgetvalue(res, 0, 0),
                                        PQgetvalue(res, 0, 1));
            }
            PQclear(res);
        }

        // Check if callback wants to stop before starting
        if (stream->stream_stop(stream->startpos, stream->timeline, false))
            return true;

        // Start replication from current position
        snprintf(query, sizeof(query), "START_REPLICATION %s%X/%X TIMELINE %u",
                slotcmd, LSN_FORMAT_ARGS(stream->startpos), stream->timeline);
        res = PQexec(conn, query);
        if (PQresultStatus(res) != PGRES_COPY_BOTH) {
            pg_log_error("could not send START_REPLICATION command");
            PQclear(res);
            return false;
        }
        PQclear(res);

        // Stream WAL data
        res = HandleCopyStream(conn, stream, &stoppos);
        if (res == NULL)
            goto error;

        // Handle end of streaming
        if (PQresultStatus(res) == PGRES_TUPLES_OK) {
            // End of timeline - transition to next timeline
            uint32 newtimeline;
            bool parsed;

            parsed = ReadEndOfStreamingResult(res, &stream->startpos, &newtimeline);
            PQclear(res);
            if (!parsed)
                goto error;

            // Validate timeline transition
            if (newtimeline <= stream->timeline || stream->startpos > stoppos) {
                pg_log_error("invalid timeline transition");
                goto error;
            }

            // Get final command result
            res = PQgetResult(conn);
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                pg_log_error("unexpected termination of replication stream");
                PQclear(res);
                goto error;
            }
            PQclear(res);

            // Restart streaming from new timeline at segment boundary
            stream->timeline = newtimeline;
            stream->startpos = stream->startpos - XLogSegmentOffset(stream->startpos, WalSegSz);
            continue;

        } else if (PQresultStatus(res) == PGRES_COMMAND_OK) {
            // Normal shutdown
            PQclear(res);
            if (stream->stream_stop(stoppos, stream->timeline, false))
                return true;
            else {
                pg_log_error("replication terminated before stop point");
                goto error;
            }
        } else {
            // Server error
            pg_log_error("unexpected termination: %s", PQresultErrorMessage(res));
            PQclear(res);
            goto error;
        }
    }

error:
    // Cleanup on error
    if (walfile != NULL && stream->walmethod->ops->close(walfile, CLOSE_NO_RENAME) != 0)
        pg_log_error("could not close file \"%s\"", walfile->pathname);
    walfile = NULL;
    return false;
}
```