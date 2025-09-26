# XLogWalRcvSendReply

## Location
[src/backend/replication/walreceiver.c:1100-1168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiver.c#L1100-L1168)

## Overview
Sends status reply messages to the primary server during streaming replication, reporting current WAL write, flush, and apply positions along with timing information.

## Definition
```c
static void XLogWalRcvSendReply(bool force, bool requestReply)
```

## Detailed Description
This function implements the status reporting mechanism between WAL receiver and primary server in PostgreSQL streaming replication. It constructs and sends reply messages containing the replica's current progress information, enabling the primary to track replication lag and make informed decisions about WAL retention.

The function implements intelligent throttling to avoid overwhelming the network with status messages. It only sends updates when significant progress has been made or when sufficient time has elapsed, unless forced. The message format includes:
- Write position (data received and written to disk)
- Flush position (data synchronized to persistent storage) 
- Apply position (data processed by recovery)
- Current timestamp for lag calculation
- Reply request flag for heartbeat scenarios

The function uses static variables to track the last reported positions and avoid redundant messages.

## Parameters / Member Variables
- `force`: Boolean flag to bypass normal throttling and send reply immediately
- `requestReply`: Boolean flag requesting the primary server to respond immediately (used for heartbeats)

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [WalRcvComputeNextWakeup](../W/WalRcvComputeNextWakeup.md)
  - [GetXLogReplayRecPtr](../G/GetXLogReplayRecPtr.md)
  - [resetStringInfo](../r/resetStringInfo.md)
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendint64](../p/pq_sendint64.md)
  - walrcv_send
- Called from (representative examples):
  - [WalReceiverMain](../W/WalReceiverMain.md)
  - [XLogWalRcvProcessMsg](XLogWalRcvProcessMsg.md)
  - [XLogWalRcvFlush](XLogWalRcvFlush.md)

## Notes and Other Information
- This is a static function internal to the walreceiver.c module
- Implements throttling based on wal_receiver_status_interval configuration
- Uses static variables to track last sent positions and avoid redundant messages
- Apply position requires spinlock access, so it's only updated periodically for performance
- Critical for monitoring replication lag and maintaining primary-replica communication
- Located in src/backend/replication/walreceiver.c:1100-1168