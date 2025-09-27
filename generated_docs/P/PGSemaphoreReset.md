# PGSemaphoreReset

## Location
[src/backend/port/posix_sema.c:295-319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/posix_sema.c#L295-L319)

## Overview
Resets a previously-initialized PostgreSQL semaphore to have a count of 0, effectively making it unavailable for locking until unlocked.

## Definition
```c
void PGSemaphoreReset(PGSemaphore sema);
```

## Detailed Description
PGSemaphoreReset decrements a semaphore's count to zero using repeated sem_trywait() calls since POSIX doesn't provide a direct API for resetting semaphore counts. The function continues calling sem_trywait() in a loop until the semaphore count reaches zero, indicated by EAGAIN or EDEADLK errno values. This ensures that any subsequent PGSemaphoreLock() call will block until the semaphore is explicitly unlocked with PGSemaphoreUnlock().

The function handles interrupts (EINTR) by retrying the operation, and reports fatal errors for any unexpected errno values. This "ratcheting down" approach is necessary because POSIX semaphores don't allow direct manipulation of the count value.

## Parameters / Member Variables
- `sema`: The PGSemaphore to reset to count 0

## Dependencies
- Functions called/Symbols referenced:
  - sem_trywait (POSIX semaphore try-wait function)
  - PG_SEM_REF (macro for getting semaphore reference)
  - elog (error reporting with FATAL level)
  - errno constants: EAGAIN, EDEADLK, EINTR
- Called from (representative examples):
  - [InitProcess](../I/InitProcess.md) (process initialization)
  - [InitAuxiliaryProcess](../I/InitAuxiliaryProcess.md) (auxiliary process initialization)

## Notes and Other Information
- Uses a "ratcheting down" approach since POSIX provides no direct reset API
- The function loops until sem_trywait() returns EAGAIN or EDEADLK, indicating count is 0
- Handles EINTR (interrupt) errors by continuing the loop
- Any other errno values result in a FATAL error
- After reset, the semaphore will block any lock attempts until explicitly unlocked
- This is commonly used during process initialization to ensure semaphores start in a known state

## Simplified Source

```c
// Simplified version of PGSemaphoreReset
void PGSemaphoreReset(PGSemaphore sema) {
    // Core logic: Repeatedly try to decrement semaphore until count reaches 0
    for (;;) {
        // Try to decrement the semaphore without blocking
        if (sem_trywait(PG_SEM_REF(sema)) < 0) {
            // Check if semaphore is already at 0 (success condition)
            if (errno == EAGAIN || errno == EDEADLK) {
                break; // Semaphore count is now 0
            }

            // Handle interrupts by retrying
            if (errno == EINTR) {
                continue;
            }

            // Any other error is fatal
            elog(FATAL, "sem_trywait failed: %m");
        }
        // If sem_trywait succeeded, continue looping to decrement further
    }
}
```

Key simplifications made:
- Added explanatory comments for each major step
- Clarified the loop's purpose (ratcheting down to 0)
- Explained the success condition (EAGAIN/EDEADLK means count is 0)
- Simplified error handling logic with clearer comments
- Maintained the essential "ratcheting down" algorithm
- Preserved all critical error handling paths