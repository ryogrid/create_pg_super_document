# GetLatestXTime

## Location
[src/backend/access/transam/xlogrecovery.c:4597-4614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4597-L4614)

## Overview
GetLatestXTime retrieves the timestamp of the latest processed commit/abort record during PostgreSQL recovery operations.

## Definition
```c
TimestampTz GetLatestXTime(void)
```

## Detailed Description
This function fetches the timestamp of the most recently processed commit or abort record from the shared XLogRecoveryCtl structure. It provides thread-safe access to the recoveryLastXTime field by using spinlock protection. The function is designed to be callable from various processes, including the checkpointer and other backend processes that need to know the timestamp of the latest transaction processed during recovery.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire (for thread-safe access)
  - SpinLockRelease (for thread-safe access)  
  - XLogRecoveryCtl (global recovery control structure)
- Called from (representative examples):
  - [CreateRestartPoint](../C/CreateRestartPoint.md)
  - [pg_last_xact_replay_timestamp](../p/pg_last_xact_replay_timestamp.md)
  - [PerformWalRecovery](../P/PerformWalRecovery.md)
  - [EndOfWalRecoveryInfo](../E/EndOfWalRecoveryInfo.md)

## Notes and Other Information
- Returns a TimestampTz value representing the timestamp of the latest processed transaction
- Uses spinlock mechanism to ensure thread-safe reads from the shared recovery control structure
- Complementary function to SetLatestXTime - this retrieves what SetLatestXTime stores
- Used by various components including checkpointing, SQL functions, and recovery coordination
- The timestamp comes from the recoveryLastXTime field in XLogRecoveryCtl