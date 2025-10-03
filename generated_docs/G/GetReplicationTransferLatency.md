# GetReplicationTransferLatency

## Location
[src/backend/replication/walreceiverfuncs.c:394-407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiverfuncs.c#L394-L407)

## Overview
Calculates and returns the network transfer latency in milliseconds between the primary and standby servers during WAL replication.

## Definition

```c
int
GetReplicationTransferLatency(void)
```
## Detailed Description
This function measures the network latency by calculating the time difference between when a message was sent from the primary server (lastMsgSendTime) and when it was received by the standby server (lastMsgReceiptTime). The measurement includes actual network transmission time plus any clock differences and timezone variations between the servers. This metric is essential for monitoring replication performance and diagnosing network-related issues in streaming replication setups.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [WalRcvData](../W/WalRcvData.md) (shared memory structure)
  - [TimestampDifferenceMilliseconds](../T/TimestampDifferenceMilliseconds.md) (time difference calculation)
- Called from (representative examples):
  - [ProcessWalSndrMessage](../P/ProcessWalSndrMessage.md) (multiple calls in walreceiver process)

## Notes and Other Information
- Returns transfer latency in milliseconds based on message timestamps
- Includes clock differences and timezone variations between servers
- Thread-safe through spinlock protection of walrcv mutex
- Used for monitoring network performance in replication environments
- Complementary to GetReplicationApplyDelay which measures apply lag
- Essential metric for diagnosing replication bottlenecks
- May show negative values if clocks are significantly out of sync
- Located in src/backend/replication/walreceiverfuncs.c:394-407

## Simplified Source

```c
// Simplified version of GetReplicationTransferLatency
int GetReplicationTransferLatency(void) {
    WalRcvData *walrcv = WalRcv;
    TimestampTz lastMsgSendTime;
    TimestampTz lastMsgReceiptTime;

    // Get message timestamps with spinlock protection
    SpinLockAcquire(&walrcv->mutex);
    lastMsgSendTime = walrcv->lastMsgSendTime;
    lastMsgReceiptTime = walrcv->lastMsgReceiptTime;
    SpinLockRelease(&walrcv->mutex);

    // Calculate network transfer latency in milliseconds
    return TimestampDifferenceMilliseconds(lastMsgSendTime, lastMsgReceiptTime);
}
```

Key simplifications made:
- Added clear comments explaining the latency calculation
- Preserved essential spinlock protection for shared memory access
- Function is already simple, minimal changes needed
- Maintained the core timestamp difference calculation