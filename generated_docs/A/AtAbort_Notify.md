# AtAbort_Notify

## Location
[src/backend/commands/async.c:1671-1690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L1671-L1690)

## Overview
Transaction abort cleanup function that removes pending notification actions and handles listener deregistration when transactions containing LISTEN/NOTIFY operations are rolled back.

## Definition

```c
void
AtAbort_Notify(void)
```
## Detailed Description
This function is called during transaction abort processing to clean up notification-related state that was established during the aborted transaction. It handles a specific edge case where a backend has registered as a listener (via LISTEN command) but the transaction is rolled back after PreCommit_Notify has been called, leaving the backend registered in the queue but without corresponding entries in the local listenChannels list. The function ensures proper cleanup by deregistering such orphaned listeners and clearing all pending notification actions and outbound notifications that would have been executed upon successful transaction commit.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [asyncQueueUnregister](../a/asyncQueueUnregister.md) (removes backend from listener queue when orphaned)
  - [ClearPendingActionsAndNotifies](../C/ClearPendingActionsAndNotifies.md) (cleans up all pending notification state)
  - amRegisteredListener (global flag indicating listener registration status)
  - listenChannels (list of channels the backend is listening on)
  - NIL (PostgreSQL's NULL list constant)
- Called from:
  - [AbortTransaction](AbortTransaction.md) (part of transaction abort processing in xact.c:2860)
  - ASYNC_H (declared in async.h:38 header file)

## Notes and Other Information
- Part of PostgreSQL's transaction abort cleanup mechanism
- Handles the specific case where LISTEN registration occurred but transaction rolls back
- Ensures no orphaned listener registrations remain in the notification queue
- Cleans up all pending notification state to maintain consistency
- Public function declared in async.h and called from transaction management code
- Critical for preventing notification queue corruption during transaction rollbacks
- Works in conjunction with other transaction lifecycle functions (AtCommit_Notify, PreCommit_Notify)
- Ensures that aborted transactions leave no notification-related artifacts behind

## Simplified Source

```c
// Simplified version of AtAbort_Notify
void AtAbort_Notify(void) {
    // Handle orphaned listener registration case
    // If we registered as a listener but have no channels (due to rollback),
    // we need to deregister to prevent queue corruption
    if (amRegisteredListener && listenChannels == NIL) {
        asyncQueueUnregister();
    }

    // Clean up all pending notification actions and outbound notifications
    ClearPendingActionsAndNotifies();
}
```

Key simplifications made:
- Added descriptive comments explaining the orphaned listener scenario
- Simplified the complex comment into clear, action-oriented descriptions
- Preserved the essential two-step logic: handle edge case, then clean up
- Maintained the original function signature and core functionality