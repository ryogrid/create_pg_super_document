# PreCommit_Notify

## Location
src/backend/commands/async.c: 861 - 967

## Overview
A pre-commit hook that processes pending LISTEN/UNLISTEN actions and queues outbound NOTIFY messages before transaction commit to ensure proper ordering and atomicity of notification operations.

## Definition
```c
void PreCommit_Notify(void)
```

## Detailed Description
This function is called during transaction commit, before the transaction is actually committed to the commit log (clog). It performs critical setup operations to ensure that notification-related operations are properly prepared for commit. The function handles two main responsibilities:

1. **Listen Registration**: For pending LISTEN operations, it ensures the backend is registered in the shared-memory listener array before commit. This prevents missing notifications from transactions that commit immediately after the current one.

2. **Notification Queuing**: For outbound NOTIFY requests, it adds them to the global notification queue before commit. This allows error handling if the queue becomes full, since the transaction can still be rolled back at this point.

The function uses serialization through heavyweight locks to ensure notifications appear in commit order and prevent uncommitted entries from blocking deliverable notifications.

## Parameters / Member Variables
- No input parameters
- Returns: `void`

## Dependencies
- Functions called/Symbols referenced:
  - `elog()` - Logging function with DEBUG1 level
  - `ListenAction` - Structure representing queued LISTEN/NOTIFY operations
  - `LISTEN_LISTEN`, `LISTEN_UNLISTEN`, `LISTEN_UNLISTEN_ALL` - Action type constants
  - `Exec_ListenPreCommit()` - Registers backend in shared listener array
  - `GetCurrentTransactionId()` - Assigns XID to current transaction
  - `LockSharedObject()` - Acquires heavyweight lock for serialization
  - `AccessExclusiveLock` - Lock mode constant
  - `list_head()` - Gets first element of PostgreSQL List
  - `asyncQueueFillWarning()` - Issues warning when queue is getting full
  - `asyncQueueIsFull()` - Checks if notification queue is full
  - `asyncQueueAddEntries()` - Adds notification entries to the queue
  - `LWLockAcquire()`, `LWLockRelease()` - Light-weight locking functions
  - `pendingActions`, `pendingNotifies` - Global variables tracking pending operations
- Called from:
  - `CommitTransaction()` - Main transaction commit function
  - Referenced in `src/include/commands/async.h` - Header file declaration

## Notes and Other Information
- Critical for maintaining notification ordering and preventing race conditions
- Uses heavyweight locks on "database 0" for serialization (historical implementation detail)
- Processes notifications page by page to allow concurrent readers
- Can still roll back the transaction if queue becomes full
- Only processes LISTEN actions in pre-commit; UNLISTEN actions are handled at commit
- The function doesn't clear pendingNotifies - that's done by AtCommit_Notify
- Location: src/backend/commands/async.c:861-967
- Part of PostgreSQL's two-phase notification commit protocol