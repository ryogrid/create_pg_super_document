# FinishStrongLockAcquire

## Location
src/backend/storage/lmgr/lock.c: 1750 - 1759

## Overview
FinishStrongLockAcquire cancels pending cleanup for a strong lock acquisition once the lock has been successfully acquired and cleanup is no longer needed.

## Definition
```c
static void FinishStrongLockAcquire(void)
```

## Detailed Description
FinishStrongLockAcquire is a simple cleanup function that resets the global StrongLockInProgress variable to NULL, indicating that no strong lock acquisition is currently in progress. This function is called after a strong lock has been successfully acquired, signaling that the error cleanup mechanism set up by BeginStrongLockAcquire is no longer needed.

The function serves as the successful completion counterpart to BeginStrongLockAcquire, ensuring that the global state tracking strong lock acquisitions is properly maintained. By clearing StrongLockInProgress, it allows future strong lock acquisitions to proceed and ensures that any error handling code knows there's no pending strong lock to clean up.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - None (only accesses global variable)
- Global variables used:
  - StrongLockInProgress
- Called from (representative examples):
  - LockAcquireExtended

## Notes and Other Information
- This is a static function only accessible within lock.c
- Must be called after a successful strong lock acquisition initiated by BeginStrongLockAcquire
- Very simple function that only resets a global pointer to NULL
- Part of the strong lock acquisition cleanup mechanism
- Should be called only when the strong lock acquisition has completed successfully
- The counterpart function AbortStrongLockAcquire handles the error cleanup case