# HotStandbyActive

## Location
src/backend/access/transam/xlogrecovery.c: 4503 - 4527

## Overview
Determines whether Hot Standby mode is currently active, providing a thread-safe way to check if standby queries can be executed.

## Definition
```c
bool HotStandbyActive(void)
```

## Detailed Description
This function checks whether PostgreSQL's Hot Standby feature is currently active. Hot Standby allows read-only queries to be executed on a standby server while it's recovering from WAL records. The function provides a safe way for any process connected to shared memory to determine the current Hot Standby status.

The function uses a caching mechanism with a local variable `LocalHotStandbyActive` to avoid repeated spinlock acquisitions once Hot Standby is known to be active. Since Hot Standby cannot be deactivated once activated during recovery, the cached value remains valid for the lifetime of the process.

The implementation uses a spinlock to safely read the shared state variable `XLogRecoveryCtl->SharedHotStandbyActive`, which is essential on machines with weak memory ordering to ensure memory consistency.

## Parameters / Member Variables
This function takes no parameters and returns a boolean value indicating Hot Standby status.

## Dependencies
- Functions called/Symbols referenced:
  - LocalHotStandbyActive (local static variable)
  - XLogRecoveryCtl->info_lck (spinlock)
  - XLogRecoveryCtl->SharedHotStandbyActive (shared memory variable)
  - SpinLockAcquire
  - SpinLockRelease
- Called from (representative examples):
  - [XLogWalRcvSendHSFeedback](../X/XLogWalRcvSendHSFeedback.md)
  - Referenced in EndOfWalRecoveryInfo

## Notes and Other Information
- Important for special backends since normal backends cannot connect until Hot Standby is active
- Works in any process connected to shared memory, unlike testing standbyState directly
- Uses caching optimization to avoid repeated spinlock acquisitions after Hot Standby becomes active
- The postmaster learns about Hot Standby status via signals, not shared memory
- Critical for determining when read-only queries can be safely executed on standby servers