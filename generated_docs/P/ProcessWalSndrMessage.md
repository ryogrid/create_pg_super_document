# ProcessWalSndrMessage

## Location
[src/backend/replication/walreceiver.c:1265-1316](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiver.c#L1265-L1316)

## Overview
Updates shared memory status information when receiving a message from the primary server during WAL replication, tracking latency and timing metrics.

## Definition

```c
static void
ProcessWalSndrMessage(XLogRecPtr walEnd, TimestampTz sendTime)
```
## Detailed Description
This function is responsible for updating the WAL receiver's shared memory state whenever a message is received from the primary server (WAL sender). It maintains critical replication status information including the latest WAL position received, message timing data, and computes replication latency metrics.

The function performs thread-safe updates to the WalRcvData shared memory structure using spinlocks, ensuring consistent state across processes. It also provides detailed debug logging that includes send times, receipt times, replication apply delays, and transfer latency measurements when DEBUG2 logging is enabled.

This function is essential for monitoring replication performance and providing visibility into the WAL streaming process between primary and standby servers.

## Parameters / Member Variables
- : XLogRecPtr indicating the end position of WAL data reported by the primary server
- : TimestampTz representing when the message was sent by the primary server

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - SpinLockAcquire
  - SpinLockRelease
  - [message_level_is_interesting](../m/message_level_is_interesting.md)
  - [timestamptz_to_str](../t/timestamptz_to_str.md)
  - [pstrdup](../p/pstrdup.md)
  - [GetReplicationApplyDelay](../G/GetReplicationApplyDelay.md)
  - [GetReplicationTransferLatency](../G/GetReplicationTransferLatency.md)
  - elog
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [XLogWalRcvProcessMsg](../X/XLogWalRcvProcessMsg.md)

## Notes and Other Information
- Updates are protected by spinlocks to ensure atomicity in a multi-process environment
- The latestWalEndTime is only updated when walEnd advances beyond the current latestWalEnd
- Debug logging includes both apply delay and transfer latency metrics when available
- Apply delay may not be available in all circumstances (returned as -1)
- String duplication is necessary because timestamptz_to_str returns a static buffer
- This function is called for each message received from the primary during active replication