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
- : PostgreSQL connection object for sending control messages
- : StreamCtl structure containing WAL method configuration and callback functions
- : Buffer containing the XLogData message
- : Length of the message buffer
- : Pointer to current block position, updated as data is processed

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