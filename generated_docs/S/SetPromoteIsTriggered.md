# SetPromoteIsTriggered

## Location
[src/backend/access/transam/xlogrecovery.c:4413-4433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4413-L4433)

## Overview
Sets the promotion trigger flag to indicate that standby promotion has been initiated, both in shared memory and locally.

## Definition

```c
static void
SetPromoteIsTriggered(void)
```
## Detailed Description
This function is responsible for setting the promotion trigger state when a standby PostgreSQL server needs to be promoted to primary. It performs two critical operations: first, it sets the shared promotion flag in  under spinlock protection to ensure thread-safe access across processes. Second, it automatically ends any recovery pause state since promotion takes precedence over paused recovery, preventing the confusing scenario where  might return 'paused' during an active promotion. Finally, it sets the local promotion flag  for quick local access without needing to acquire locks.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - [SetRecoveryPause](SetRecoveryPause.md)
  - XLogRecoveryCtl (global variable)
- Called from (representative examples):
  - [CheckForStandbyTrigger](../C/CheckForStandbyTrigger.md)

## Notes and Other Information
- This function is static and only accessible within the xlogrecovery.c file
- Uses spinlock protection to ensure atomic updates to shared memory state
- Automatically clears recovery pause state to prevent inconsistent state reporting during promotion
- Sets both shared and local promotion flags for efficient access patterns
- Located at src/backend/access/transam/xlogrecovery.c:4413-4433

## Simplified Source

```c
static void
SetPromoteIsTriggered(void)
{
    // Set shared promotion flag under spinlock protection
    SpinLockAcquire(&XLogRecoveryCtl->info_lck);
    XLogRecoveryCtl->SharedPromoteIsTriggered = true;
    SpinLockRelease(&XLogRecoveryCtl->info_lck);

    // Clear recovery pause state since promotion takes precedence
    SetRecoveryPause(false);

    // Set local promotion flag for quick access
    LocalPromoteIsTriggered = true;
}
```