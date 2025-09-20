# GetReplicationApplyDelay

## Location
[src/backend/replication/walreceiverfuncs.c:364-393](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiverfuncs.c#L364-L393)

## Overview
Calculates and returns the replication apply delay in milliseconds, representing the time lag between receiving WAL data and applying it during recovery.

## Definition

```c
int
GetReplicationApplyDelay(void)
```
## Detailed Description
This function measures the replication lag by calculating the time difference between when the current WAL chunk started replaying and the current time. It compares the flushed receive position with the current replay position to determine if there's any lag. If the positions are equal (no lag), it returns 0. If chunk replay timing information is unavailable, it returns -1. The function provides crucial metrics for monitoring replication performance and identifying bottlenecks in standby servers.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [WalRcvData](../W/WalRcvData.md) (shared memory structure)
  - [GetXLogReplayRecPtr](GetXLogReplayRecPtr.md) (current replay position)
  - [GetCurrentChunkReplayStartTime](GetCurrentChunkReplayStartTime.md) (when current chunk started replaying)
  - [TimestampDifferenceMilliseconds](../T/TimestampDifferenceMilliseconds.md) (time difference calculation)
  - [GetCurrentTimestamp](GetCurrentTimestamp.md) (current system time)
- Called from (representative examples):
  - [ProcessWalSndrMessage](../P/ProcessWalSndrMessage.md) (in walreceiver process)

## Notes and Other Information
- Returns delay in milliseconds, 0 if no delay, or -1 if information unavailable
- Uses spinlock protection when accessing WAL receiver shared memory
- Essential for replication monitoring and performance tuning
- Measures apply lag, not transfer lag (which is measured separately)
- Used in replication status reporting and monitoring systems
- Helps identify when standby servers are falling behind in applying WAL
- Located in src/backend/replication/walreceiverfuncs.c:364-393