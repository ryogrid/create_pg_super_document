# WaitOnLock

## Location
[src/backend/storage/lmgr/lock.c:1818-1907](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L1818-L1907)

## Overview
WaitOnLock causes the current process to sleep until a requested lock can be acquired, handling deadlock detection and process status updates during the wait.

## Definition

```c
static void
WaitOnLock(LOCALLOCK *locallock, ResourceOwner owner, bool dontWait)
```
## Detailed Description
WaitOnLock is a critical function in PostgreSQL's lock management system that handles the process of waiting for a lock to become available. The function sets up the necessary state for the current process to sleep until the requested lock can be granted, while properly handling potential deadlocks and maintaining process status information.

Key aspects of the function:
- Updates the process title to indicate waiting status
- Sets global variables (awaitedLock, awaitedOwner) for deadlock detection
- Uses PG_TRY/PG_CATCH for proper cleanup on interruption
- Calls ProcSleep to perform the actual waiting
- Handles deadlock detection and reporting
- Ensures proper cleanup of waiting state

The function is designed with careful consideration of interrupt handling and state consistency, ensuring that the lock state remains coherent even if the process is interrupted during the wait.

## Parameters / Member Variables
- `*locallock`: Pointer to the LOCALLOCK structure representing the lock being waited for
- `owner`: ResourceOwner that will own the lock once acquired
- `dontWait`: If true, don't actually wait but set up state as if waiting (for conditional lock attempts)
## Dependencies
- Functions called/Symbols referenced:
  - LOCALLOCK_LOCKMETHOD (macro)
  - LockMethods (global array)
  - LOCK_PRINT (debug macro)
  - [set_ps_display_suffix](../s/set_ps_display_suffix.md)
  - [ProcSleep](../P/ProcSleep.md)
  - LockHashPartitionLock
  - [DeadLockReport](../D/DeadLockReport.md)
  - [set_ps_display_remove_suffix](../s/set_ps_display_remove_suffix.md)
  - PG_TRY/PG_CATCH/PG_END_TRY (exception handling)
  - PROC_WAIT_STATUS_OK (constant)
- Called from (representative examples):
  - [LockAcquireExtended](../L/LockAcquireExtended.md)
  - PROCLOCK_PRINT (debug context)

## Notes and Other Information
- This is a static function, only callable within lock.c
- Requires the appropriate partition lock to be held at entry and maintains it at exit
- Critical section: no shared-state cleanup should occur after ProcSleep call
- The awaitedLock and awaitedOwner globals are used by deadlock detection code
- Process title is temporarily modified to show waiting status
- Handles both blocking and non-blocking (dontWait) scenarios
- Located in src/backend/storage/lmgr/lock.c at lines 1818-1907
- Essential cleanup must happen in LockErrorCleanup, not in this function's exception handler

## Simplified Source

```c
// Simplified version of WaitOnLock
static void
WaitOnLock(LOCALLOCK *locallock, ResourceOwner owner, bool dontWait)
{
    LOCKMETHODID lockmethodid = LOCALLOCK_LOCKMETHOD(*locallock);
    LockMethod lockMethodTable = LockMethods[lockmethodid];

    // Update process title to show waiting status
    set_ps_display_suffix("waiting");

    // Set global variables for deadlock detection
    awaitedLock = locallock;
    awaitedOwner = owner;

    // Use exception handling to ensure cleanup on interruption
    PG_TRY();
    {
        // Sleep until lock becomes available or deadlock detected
        if (ProcSleep(locallock, lockMethodTable, dontWait) != PROC_WAIT_STATUS_OK
            && !dontWait)
        {
            // Deadlock detected - clean up and report
            awaitedLock = NULL;
            LWLockRelease(LockHashPartitionLock(locallock->hashcode));
            DeadLockReport(); // Does not return
        }
    }
    PG_CATCH();
    {
        // Reset process display on error and re-throw
        set_ps_display_remove_suffix();
        PG_RE_THROW();
    }
    PG_END_TRY();

    // Lock acquired successfully - clean up waiting state
    awaitedLock = NULL;
    set_ps_display_remove_suffix();
}
```

Key simplifications made:
- Removed detailed comments about interrupt handling and state management
- Simplified the deadlock handling path while preserving essential logic
- Consolidated debug printing statements
- Streamlined the exception handling structure
- Maintained all critical operations: process title updates, global variable management, ProcSleep call, deadlock detection, and cleanup