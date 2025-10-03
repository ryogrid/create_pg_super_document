# WalRcvForceReply

## Location
[src/backend/replication/walreceiver.c:1358-1375](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiver.c#L1358-L1375)

## Overview
Wakes up the WAL receiver process to trigger immediate sending of apply notifications to the primary server, particularly for synchronous replication scenarios.

## Definition

```c
void
WalRcvForceReply(void)
```
## Detailed Description
This function provides a mechanism for the startup process to signal the WAL receiver that it should immediately send a reply message to the primary server. This is particularly important for synchronous replication scenarios where the primary server may be waiting for confirmation that WAL records have been applied on the standby before completing operations like COMMIT with synchronous_commit = remote_apply.

The function sets a force_reply flag in the shared WalRcvData structure and then wakes up the WAL receiver process by setting its latch. The latch access is protected by a spinlock to ensure thread safety, as the latch pointer fetch might not be atomic in a multi-process environment.

This mechanism enables responsive synchronous replication by ensuring that apply notifications are sent promptly when significant WAL records are processed, rather than waiting for the next scheduled status interval.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - [SetLatch](../S/SetLatch.md)
  - [Latch](../L/Latch.md) (data type)
- Called from (representative examples):
  - [ApplyWalRecord](../A/ApplyWalRecord.md)
  - [WaitForWALToBecomeAvailable](WaitForWALToBecomeAvailable.md)

## Notes and Other Information
- This function is called by the startup process when applying interesting XLog records
- The force_reply flag is checked by the WAL receiver main loop to trigger immediate replies
- [Latch](../L/Latch.md) access is protected by spinlocks because fetching the latch pointer might not be atomic
- Critical for synchronous replication performance, particularly with remote_apply synchronous_commit levels
- The function is non-blocking and simply signals the WAL receiver; the actual reply sending happens in the main WAL receiver loop
- Thread-safe design allows safe calling from different processes in the PostgreSQL multi-process architecture

## Simplified Source

```c
void WalRcvForceReply(void)
{
    Latch *latch;

    // Set flag to force immediate reply to primary
    WalRcv->force_reply = true;

    // Safely get the latch pointer (might not be atomic)
    SpinLockAcquire(&WalRcv->mutex);
    latch = WalRcv->latch;
    SpinLockRelease(&WalRcv->mutex);

    // Wake up the WAL receiver process if it exists
    if (latch)
        SetLatch(latch);
}
```