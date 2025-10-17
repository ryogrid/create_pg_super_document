# ProcessXLogDataMsg

## Location
[src/bin/pg_basebackup/receivelog.c:1040-1170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/receivelog.c#L1040-L1170)

## Overview
ProcessXLogDataMsg processes XLogData messages containing actual WAL (Write-Ahead Log) data from streaming replication and writes the data to WAL files.

## Definition

```c
static bool
ProcessXLogDataMsg(PGconn *conn, StreamCtl *stream, char *copybuf, int len,
				   XLogRecPtr *blockpos)
```
## Detailed Description
This function is the core handler for actual WAL data received during streaming replication. It parses XLogData message headers to extract the WAL location information, validates that received data aligns with expected positions, and writes the data to appropriate WAL files. The function handles WAL segment boundaries by automatically closing completed segments and opening new ones as needed. It also implements position tracking to ensure data continuity and can terminate streaming when a configured stop condition is met. The function carefully manages file operations and handles potential write errors.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection object for sending control messages
- `*stream`: StreamCtl structure containing WAL method configuration and callback functions
- `*copybuf`: Buffer containing the XLogData message
- `len`: Length of the message buffer
- `*blockpos`: Pointer to current block position, updated as data is processed
## Dependencies
- Functions called/Symbols referenced:
  - [fe_recvint64](../f/fe_recvint64.md)
  - XLogSegmentOffset
  - [open_walfile](../o/open_walfile.md)
  - [GetLastWalMethodError](../G/GetLastWalMethodError.md)
  - [close_walfile](../c/close_walfile.md)
  - [PQputCopyEnd](PQputCopyEnd.md)
  - [PQflush](PQflush.md)
  - [PQerrorMessage](PQerrorMessage.md)
  - pg_log_error
- Called from (representative examples):
  - [HandleCopyStream](../H/HandleCopyStream.md)

## Notes and Other Information
- Returns true on success, false on failure
- XLogData message format: msgtype(1) + dataStart(8) + walEnd(8) + sendTime(8) + data
- Automatically handles WAL segment boundaries (typically 16MB segments)
- Validates write position continuity to detect streaming inconsistencies
- Can terminate streaming gracefully when stop condition callback returns true
- Ignores subsequent messages when still_sending flag is false
- Critical component for maintaining WAL file integrity during base backup and streaming replication

## Simplified Source

```c
static bool
ProcessXLogDataMsg(PGconn *conn, StreamCtl *stream, char *copybuf, int len,
                   XLogRecPtr *blockpos)
{
    int xlogoff;
    int bytes_left;
    int bytes_written;
    int hdr_len;

    // Ignore if no longer receiving
    if (!still_sending)
        return true;

    // Parse XLogData header: msgtype(1) + dataStart(8) + walEnd(8) + sendTime(8)
    hdr_len = 1 + 8 + 8 + 8;
    if (len < hdr_len) {
        pg_log_error("streaming header too small: %d", len);
        return false;
    }
    *blockpos = fe_recvint64(&copybuf[1]);  // Extract dataStart

    // Calculate offset within WAL segment
    xlogoff = XLogSegmentOffset(*blockpos, WalSegSz);

    // Validate position continuity
    if (walfile == NULL) {
        if (xlogoff != 0) {
            pg_log_error("received write-ahead log record for offset %u with no file open", xlogoff);
            return false;
        }
    } else {
        if (walfile->currpos != xlogoff) {
            pg_log_error("got WAL data offset %08x, expected %08x", xlogoff, (int) walfile->currpos);
            return false;
        }
    }

    // Write data to WAL files
    bytes_left = len - hdr_len;
    bytes_written = 0;

    while (bytes_left) {
        // Don't cross WAL segment boundaries
        int bytes_to_write = (xlogoff + bytes_left > WalSegSz) ?
                             WalSegSz - xlogoff : bytes_left;

        // Open WAL file if needed
        if (walfile == NULL) {
            if (!open_walfile(stream, *blockpos))
                return false;
        }

        // Write data chunk
        if (stream->walmethod->ops->write(walfile, copybuf + hdr_len + bytes_written,
                                          bytes_to_write) != bytes_to_write) {
            pg_log_error("could not write %d bytes to WAL file \"%s\": %s",
                         bytes_to_write, walfile->pathname,
                         GetLastWalMethodError(stream->walmethod));
            return false;
        }

        // Update positions
        bytes_written += bytes_to_write;
        bytes_left -= bytes_to_write;
        *blockpos += bytes_to_write;
        xlogoff += bytes_to_write;

        // Handle WAL segment boundary
        if (XLogSegmentOffset(*blockpos, WalSegSz) == 0) {
            if (!close_walfile(stream, *blockpos))
                return false;

            xlogoff = 0;

            // Check for stop condition
            if (still_sending && stream->stream_stop(*blockpos, stream->timeline, true)) {
                if (PQputCopyEnd(conn, NULL) <= 0 || PQflush(conn)) {
                    pg_log_error("could not send copy-end packet: %s", PQerrorMessage(conn));
                    return false;
                }
                still_sending = false;
                return true;
            }
        }
    }

    return true;
}
```