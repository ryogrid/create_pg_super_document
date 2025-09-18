# GrantLock

## Location
src/backend/storage/lmgr/lock.c: 1558 - 1580

## Overview
GrantLock updates the lock and proclock data structures to reflect that a lock request has been granted, modifying the granted counts and bitmasks appropriately.

## Definition
```c
void GrantLock(LOCK *lock, PROCLOCK *proclock, LOCKMODE lockmode)
```

## Detailed Description
This function performs the core bookkeeping required when a lock is granted to a process. It updates several critical data structures:

1. **Global Lock Counters**: Increments the total granted count (`nGranted`) and the specific mode count (`granted[lockmode]`)
2. **Grant Mask Updates**: Sets the appropriate bit in the lock's `grantMask` to indicate this mode is now held
3. **Wait Mask Management**: If all requested locks of this mode have been granted, clears the corresponding bit in the `waitMask`
4. **Proclock Updates**: Updates the requesting process's `holdMask` to reflect the newly granted lock mode
5. **Debugging and Validation**: Includes debugging output and assertions to verify data structure consistency

The function is designed to be called after conflict checking has determined that a lock can be granted. It does not handle wait queue management or LOCALLOCK updates, which are handled by separate functions.

## Parameters / Member Variables
- `lock`: Pointer to the LOCK structure representing the locked resource
- `proclock`: Pointer to the PROCLOCK structure representing the process-lock relationship
- `lockmode`: The specific lock mode being granted (e.g., AccessShareLock, ExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - LOCKBIT_ON (macro for setting lock mode bits)
  - LOCKBIT_OFF (macro for clearing lock mode bits)  
  - LOCK_PRINT (debugging macro)
- Called from (representative examples):
  - LockAcquireExtended
  - ProcSleep
  - ProcLockWakeup
  - FastPathTransferRelationLocks
  - VirtualXactLock

## Notes and Other Information
- This function only updates the shared memory data structures, not the local LOCALLOCK table
- The caller is responsible for removing the process from wait queues if it was waiting
- Includes assertions to verify that grant counts remain consistent and within expected bounds
- The function assumes conflict checking has already been performed and the lock can be safely granted
- Wait mask clearing is conditional - only occurs when all requests for a given mode have been satisfied
- Used in both normal lock acquisition and recovery scenarios (e.g., two-phase commit recovery)