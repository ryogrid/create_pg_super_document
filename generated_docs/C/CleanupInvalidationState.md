# CleanupInvalidationState

## Location
src/backend/storage/ipc/sinvaladt.c: 328 - 369

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
  - DatumGetPointer
  - Assert
  - PointerIsValid
  - LWLockAcquire
  - LWLockRelease
  - elog
- Data types referenced:
  - SISeg
  - ProcState
  - Datum
- Constants referenced:
  - LW_EXCLUSIVE
  - PANIC
- Variables referenced:
  - MyProcNumber
  - nextLocalTransactionId
  - SInvalWriteLock
- Called from (representative examples):
  - on_shmem_exit (automatic callback during process exit)

## Notes and Other Information
- This is a static function only used internally within the sinvaladt.c module
- The function is designed to be safe to call during abnormal process termination
- Exclusive locking ensures that cleanup operations are atomic and don't interfere with other processes
- The pgprocnos array compaction maintains a dense array of active process numbers for efficient iteration
- The PANIC condition should never occur in normal operation and indicates a serious system corruption
- Preserving nextLXID allows the process slot to be reused without local transaction ID conflicts
- The function must handle the case where the process might not be found in the pgprocnos array, which would indicate a serious bug