# CleanupInvalidationState

## Location
[src/backend/storage/ipc/sinvaladt.c:328-369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/sinvaladt.c#L328-L369)

## Overview
CleanupInvalidationState deactivates a backend process from the shared invalidation system during backend shutdown, cleaning up its registration and state.

## Definition
static void CleanupInvalidationState(int status, Datum arg)

## Detailed Description
This function is registered as a shared memory exit handler and is automatically called when a backend process terminates. It performs the following cleanup operations:

1. Converts the Datum argument back to a SISeg pointer
2. Acquires exclusive SInvalWriteLock to prevent concurrent access
3. Updates the nextLXID in the process state for the next process that may use this slot
4. Marks the process slot as inactive by clearing procPid and other state fields
5. Removes the process number from the active processes array (pgprocnos)
6. Compacts the pgprocnos array by moving the last element to fill the gap
7. Decrements the total number of active processes

The function ensures that the shared invalidation system maintains an accurate list of active processes and properly releases resources when backends exit.

## Parameters / Member Variables
- : Exit status of the process (not used in the function)
- : Datum containing a pointer to the SISeg structure, passed from the registration in SharedInvalBackendInit

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - Assert
  - PointerIsValid
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - elog
- Data types referenced:
  - [SISeg](../S/SISeg.md)
  - [ProcState](../P/ProcState.md)
  - Datum
- Constants referenced:
  - LW_EXCLUSIVE
  - PANIC
- [Variables](../V/Variables.md) referenced:
  - MyProcNumber
  - nextLocalTransactionId
  - SInvalWriteLock
- Called from (representative examples):
  - [on_shmem_exit](../o/on_shmem_exit.md) (automatic callback during process exit)

## Notes and Other Information
- This is a static function only used internally within the sinvaladt.c module
- The function is designed to be safe to call during abnormal process termination
- Exclusive locking ensures that cleanup operations are atomic and don't interfere with other processes
- The pgprocnos array compaction maintains a dense array of active process numbers for efficient iteration
- The PANIC condition should never occur in normal operation and indicates a serious system corruption
- Preserving nextLXID allows the process slot to be reused without local transaction ID conflicts
- The function must handle the case where the process might not be found in the pgprocnos array, which would indicate a serious bug

## Simplified Source

```c
// Simplified version of CleanupInvalidationState
static void CleanupInvalidationState(int status, Datum arg) {
    SISeg *segP = (SISeg *) DatumGetPointer(arg);
    ProcState *stateP;
    int i;

    // Acquire exclusive lock to prevent concurrent access
    LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);

    stateP = &segP->procState[MyProcNumber];

    // Preserve next local transaction ID for process slot reuse
    stateP->nextLXID = nextLocalTransactionId;

    // Mark process slot as inactive
    stateP->procPid = 0;
    stateP->nextMsgNum = 0;
    stateP->resetState = false;
    stateP->signaled = false;

    // Remove process from active processes array
    for (i = segP->numProcs - 1; i >= 0; i--) {
        if (segP->pgprocnos[i] == MyProcNumber) {
            // Move last element to fill gap (array compaction)
            if (i != segP->numProcs - 1)
                segP->pgprocnos[i] = segP->pgprocnos[segP->numProcs - 1];
            break;
        }
    }

    // Verify process was found in array
    if (i < 0)
        elog(PANIC, "could not find entry in sinval array");

    segP->numProcs--;

    LWLockRelease(SInvalWriteLock);
}
```

Key simplifications made:
- Preserved all essential logic and error handling
- Added descriptive comments explaining each major step
- Maintained the original structure and flow
- Clarified the array compaction logic with inline comments
- Kept critical assertions and error conditions intact