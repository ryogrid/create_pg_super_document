# AtSubAbort_Notify

## Location
src/backend/commands/async.c: 1761 - 1803

## Overview
Handles PostgreSQL LISTEN/NOTIFY operations during subtransaction abort by discarding pending actions and notifications from the aborted subtransaction.

## Definition
```c
void AtSubAbort_Notify(void)
```

## Detailed Description
This function is called during subtransaction abort to clean up pending LISTEN/NOTIFY operations that were initiated within the aborted subtransaction. Unlike AtSubCommit_Notify which preserves operations by moving them to the parent transaction, this function simply discards all pending actions and notifications at or below the current subtransaction nesting level.

The function operates by "popping the stack" - it removes ActionList and NotificationList objects from the current subtransaction level and any deeper levels. The actual action and notification data is automatically freed when CurTransactionContext is recycled, but the list container objects themselves must be explicitly freed since they are allocated in TopTransactionContext.

The function is designed to be safe for reentrant calls during error recovery, handling cases where there may be no entries at the current subtransaction level.

## Parameters / Member Variables
This function takes no parameters and operates on global state variables.

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [ActionList](ActionList.md) (structure type)
  - [NotificationList](../N/NotificationList.md) (structure type)
- Called from (representative examples):
  - [AbortSubTransaction](AbortSubTransaction.md)

## Notes and Other Information
- Uses while loops to handle multiple levels that may need cleanup during nested subtransaction aborts
- Memory management is split: list contents are freed automatically via context cleanup, but list containers require explicit pfree()
- Safe for reentrant calls during error recovery scenarios
- Part of PostgreSQL's subtransaction management system for asynchronous notifications
- Located in src/backend/commands/async.c:1761-1803