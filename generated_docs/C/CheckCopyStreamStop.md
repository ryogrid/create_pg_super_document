# CheckCopyStreamStop

## Location
[src/bin/pg_basebackup/receivelog.c:1211-1235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/receivelog.c#L1211-L1235)

## Overview
CheckCopyStreamStop determines whether the WAL streaming process should continue or terminate gracefully at a specific position during PostgreSQL base backup or replication operations.

## Definition

```c
static bool
CheckCopyStreamStop(PGconn *conn, StreamCtl *stream, XLogRecPtr blockpos)
```
## Detailed Description
This function evaluates whether the streaming process should be stopped based on the current block position and stream configuration. When the stream_stop callback indicates that streaming should cease, it performs an orderly shutdown by closing the current WAL file and sending a copy-end packet to the server. The function ensures proper cleanup of resources and communication with the PostgreSQL server during the termination process.

The function operates within the context of WAL streaming operations, typically used by pg_basebackup and similar utilities that need to receive transaction log data from a PostgreSQL server.

## Parameters / Member Variables
- : PostgreSQL connection handle for communicating with the server
- : StreamCtl structure containing stream configuration and callback functions
- : XLogRecPtr indicating the current position in the transaction log

## Dependencies
- Functions called/Symbols referenced:
  - [StreamCtl](../S/StreamCtl.md) (structure type)
  - [close_walfile](../c/close_walfile.md)
  - [PQputCopyEnd](../P/PQputCopyEnd.md)
  - [PQflush](../P/PQflush.md)
- Called from (representative examples):
  - [HandleCopyStream](../H/HandleCopyStream.md)

## Notes and Other Information
- This is a static function internal to receivelog.c, used specifically in WAL streaming contexts
- The function relies on the global variable  to track the current streaming state
- Error handling includes proper error message reporting through pg_log_error when communication with the server fails
- The function returns false on any error condition, allowing the caller to handle failures appropriately
- Part of PostgreSQL's base backup and replication infrastructure located in src/bin/pg_basebackup/