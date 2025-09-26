# WalSndKeepalive

## Location
[src/backend/replication/walsender.c:4076-4098](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L4076-L4098)

## Overview
Sends a keepalive message to the standby server to maintain the replication connection and optionally request a heartbeat response.

## Definition
```c
static void WalSndKeepalive(bool requestReply, XLogRecPtr writePtr)
```

## Detailed Description
WalSndKeepalive constructs and sends a keepalive message to the standby server as part of the PostgreSQL streaming replication protocol. The function creates a properly formatted message containing the current WAL position, timestamp, and reply request flag. The message is sent using the COPY protocol with message type 'k' for keepalive. When requestReply is true, the function sets a local flag to track that a response is expected, preventing duplicate requests until a response is received.

## Parameters / Member Variables
- `requestReply`: Boolean indicating whether the standby should send a response message back for heartbeat purposes
- `writePtr`: XLogRecPtr representing the WAL location up to which data has been written; if invalid, sentPtr is used instead

## Dependencies
- Functions called/Symbols referenced:
  - elog (with DEBUG2 level)
  - [resetStringInfo](../r/resetStringInfo.md)
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - XLogRecPtrIsInvalid
  - [pq_sendint64](../p/pq_sendint64.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - pq_putmessage_noblock
  - output_message (global StringInfo)
  - sentPtr (global variable)
  - waiting_for_ping_response (global flag)
- Called from (representative examples):
  - WALSND_LOGICAL_LAG_TRACK_INTERVAL_MS
  - [WalSndWaitForWal](WalSndWaitForWal.md)
  - [ProcessStandbyReplyMessage](../P/ProcessStandbyReplyMessage.md)
  - [WalSndDone](WalSndDone.md)
  - [WalSndKeepaliveIfNecessary](WalSndKeepaliveIfNecessary.md)

## Notes and Other Information
- This is a static function, only accessible within the walsender.c file
- Sends a message with type 'k' (keepalive) wrapped in CopyData protocol
- Message format includes: WAL position (8 bytes), timestamp (8 bytes), reply request flag (1 byte)
- Uses non-blocking message sending to avoid stalling the WAL sender process
- The waiting_for_ping_response flag prevents sending duplicate keepalive requests
- Used for connection health monitoring and flow control in streaming replication
- Located in src/backend/replication/walsender.c at lines 4076-4098