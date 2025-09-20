# XLogShutdownWalRcv

## Location
[src/backend/access/transam/xlog.c:9489-9499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L9489-L9499)

## Overview
A thin wrapper function that shuts down the WAL receiver process and resets the InstallXLogFileSegmentActive flag to ensure clean WAL receiver shutdown.

## Definition

```c
void
XLogShutdownWalRcv(void)
```
## Detailed Description
This function provides a coordinated shutdown of the WAL receiver subsystem. It first calls ShutdownWalRcv() to terminate the WAL receiver process, then safely resets the InstallXLogFileSegmentActive flag under the protection of the ControlFileLock. This ensures that any ongoing WAL file segment installation operations are properly marked as inactive during the shutdown process.

The function combines WAL receiver shutdown with the cleanup of associated state flags, providing a single point of control for proper WAL receiver termination.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [ShutdownWalRcv](../S/ShutdownWalRcv.md)
  - LWLockAcquire
  - LWLockRelease
  - XLogCtl (global control structure)
  - ControlFileLock
  - LW_EXCLUSIVE (lock mode constant)
- Called from (representative examples):
  - [FinishWalRecovery](../F/FinishWalRecovery.md) (in xlogrecovery.c)
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md) (in xlogrecovery.c)

## Notes and Other Information
- This function is part of the WAL recovery and streaming replication infrastructure
- The InstallXLogFileSegmentActive flag is used to coordinate WAL file segment operations
- Uses exclusive locking to ensure atomic updates to the control structure
- Location: src/backend/access/transam/xlog.c:9489-9499
- Essential for clean shutdown during recovery transitions and replication state changes