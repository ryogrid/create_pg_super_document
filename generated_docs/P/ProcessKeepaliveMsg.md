# ProcessKeepaliveMsg

## Location
src/bin/pg_basebackup/receivelog.c: 986 - 1039

## Overview
ProcessKeepaliveMsg processes keepalive messages from a PostgreSQL streaming replication server and sends feedback responses when requested.

## Definition


## Detailed Description
This function handles keepalive messages that are sent by the PostgreSQL server during streaming replication to maintain the connection and request status updates from the client. The function parses the keepalive message format, extracting the reply request flag while skipping other fields like walEnd and sendTime. When the server requests an immediate reply, the function sends a feedback message containing the current replication position. If flush position reporting is enabled and the current position has advanced, it synchronizes the current WAL file to ensure accurate flush position reporting. This mechanism is crucial for monitoring replication lag and ensuring data durability.

## Parameters / Member Variables
- : PostgreSQL connection object for sending feedback responses
- : StreamCtl structure containing WAL method and streaming configuration
- : Buffer containing the keepalive message data
- : Length of the message buffer
- : Current block position for replication feedback
- : Pointer to timestamp tracking the last status message sent

## Dependencies
- Functions called/Symbols referenced:
  - GetLastWalMethodError
  - feGetCurrentTimestamp
  - sendFeedback
  - pg_log_error
  - pg_fatal
- Called from (representative examples):
  - HandleCopyStream

## Notes and Other Information
- Returns true on success, false on failure
- Parses keepalive message format: msgtype(1) + walEnd(8) + sendTime(8) + replyRequested(1)
- Performs fsync on WAL file before reporting flush position to ensure data durability
- Only sends feedback when still_sending flag is true and server requests a reply
- Updates lastFlushPosition when WAL file is successfully synchronized
- Critical for maintaining streaming replication connection health and monitoring replication progress