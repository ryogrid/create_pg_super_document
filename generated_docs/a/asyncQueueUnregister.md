# asyncQueueUnregister

## Location
src/backend/commands/async.c: 1231 - 1271

## Overview
Removes the current backend's entry from the notification listeners array when it is no longer listening on any channel.

## Definition


## Detailed Description
This function safely removes the current backend from the global notification listeners array maintained in shared memory. It is called when a backend no longer has any active LISTEN channels. The function performs the following operations:

1. Verifies that the listenChannels list is empty (NIL) to ensure the caller is correct
2. Checks if the backend is actually registered as a listener before proceeding
3. Acquires exclusive NotifyQueueLock to safely manipulate the shared listener list
4. Marks the backend's entry as invalid by setting PID to InvalidPid and database OID to InvalidOid
5. Removes the backend from the linked list of listeners by updating the appropriate next pointers
6. Updates the global amRegisteredListener flag to false

The function handles both cases where the backend is the first listener in the queue and where it appears elsewhere in the linked list.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - QUEUE_BACKEND_PID
  - QUEUE_BACKEND_DBOID  
  - QUEUE_FIRST_LISTENER
  - QUEUE_NEXT_LISTENER
  - LWLockAcquire
  - LWLockRelease
  - InvalidPid
  - InvalidOid
  - INVALID_PROC_NUMBER
  - MyProcNumber
  - NotifyQueueLock
  - ProcNumber

- Called from:
  - Async_UnlistenOnExit
  - AtCommit_Notify
  - AtAbort_Notify

## Notes and Other Information
- This is a static function internal to async.c
- Must be called only when listenChannels is NIL, otherwise it will assert
- The function is safe to call even if the backend is not currently registered as a listener
- Requires exclusive lock on NotifyQueueLock to maintain consistency of the shared listener list
- Part of PostgreSQL's asynchronous notification system (LISTEN/NOTIFY)