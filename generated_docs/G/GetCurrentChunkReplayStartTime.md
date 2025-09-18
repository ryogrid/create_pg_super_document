# GetCurrentChunkReplayStartTime

## Location
[src/backend/access/transam/xlogrecovery.c:4627-4642](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4627-L4642)

## Overview
GetCurrentChunkReplayStartTime retrieves the timestamp marking the beginning of the current chunk of WAL records being processed during recovery.

## Definition
```c
TimestampTz GetCurrentChunkReplayStartTime(void)
```

## Detailed Description
This function fetches the timestamp that marks the start time of the current chunk of WAL records being replayed during recovery. The function provides thread-safe access to the currentChunkStartTime field stored in the shared XLogRecoveryCtl structure by using spinlock protection. While the comment mentions "latest processed commit/abort record" and "XLogReceiptTime", this appears to be a documentation error - the function actually returns the currentChunkStartTime, which tracks chunk boundaries rather than individual transaction timestamps.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire (for thread-safe access)
  - SpinLockRelease (for thread-safe access)
  - XLogRecoveryCtl (global recovery control structure)
- Called from (representative examples):
  - [GetReplicationApplyDelay](GetReplicationApplyDelay.md)
  - [EndOfWalRecoveryInfo](../E/EndOfWalRecoveryInfo.md)

## Notes and Other Information
- Returns a TimestampTz value representing the start time of the current WAL chunk being processed
- Uses spinlock mechanism to ensure thread-safe reads from shared recovery control structure
- Complementary function to SetCurrentChunkStartTime - this retrieves what SetCurrentChunkStartTime stores
- Used in replication monitoring and recovery coordination
- The function comment appears to be incorrect or copied from another function - it retrieves currentChunkStartTime, not the latest commit/abort timestamp
- Accessible to all backend processes, enabling system-wide visibility of current recovery chunk status