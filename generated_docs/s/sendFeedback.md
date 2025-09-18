# sendFeedback

## Location
[src/bin/pg_basebackup/receivelog.c:337-374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/receivelog.c#L337-L374)

## Overview
Sends a Standby Status Update message to the PostgreSQL server during logical replication to acknowledge the receipt and processing of WAL data.

## Definition


## Detailed Description
The  function constructs and sends a standby status update message to inform the WAL sender about the current state of logical replication. It tracks the last written and fsynced LSN positions to avoid sending superfluous feedback messages, unless forced by a timeout condition. The function builds a binary message containing LSN positions and timing information, then transmits it through the replication connection using the PostgreSQL copy protocol.

The feedback mechanism is crucial for preventing  from killing the connection and allows the server to track replication progress for lag monitoring and slot advancement.

## Parameters / Member Variables
- : PostgreSQL connection handle for the replication session
- : Current timestamp to include in the feedback message
- : Boolean flag to force sending feedback even if LSN positions haven't changed
- : Boolean indicating whether the server requested a reply

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info
  - [fe_sendint64](../f/fe_sendint64.md)
  - [PQputCopyData](../P/PQputCopyData.md)
  - [PQflush](../P/PQflush.md)
- Called from (representative examples):
  - [StreamLogicalLog](../S/StreamLogicalLog.md)
  - [flushAndSendFeedback](../f/flushAndSendFeedback.md)
  - [HandleCopyStream](../H/HandleCopyStream.md)
  - [ProcessKeepaliveMsg](../P/ProcessKeepaliveMsg.md)

## Notes and Other Information
- Uses static variables to track last_written_lsn and last_fsync_lsn to avoid redundant messages
- Constructs a 34-byte binary message with message type 'r' followed by LSN and timestamp data
- Sets apply LSN to InvalidXLogRecPtr since logical replication doesn't track apply position the same way as physical replication
- Critical for maintaining replication connection health and preventing timeouts
- Part of the PostgreSQL logical replication feedback protocol