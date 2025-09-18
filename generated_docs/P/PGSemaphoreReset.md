# PGSemaphoreReset

## Location
src/backend/port/posix_sema.c: 295 - 319

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
  - InitProcess (process initialization)
  - [InitAuxiliaryProcess](../I/InitAuxiliaryProcess.md) (auxiliary process initialization)

## Notes and Other Information
- Uses a "ratcheting down" approach since POSIX provides no direct reset API
- The function loops until sem_trywait() returns EAGAIN or EDEADLK, indicating count is 0
- Handles EINTR (interrupt) errors by continuing the loop
- Any other errno values result in a FATAL error
- After reset, the semaphore will block any lock attempts until explicitly unlocked
- This is commonly used during process initialization to ensure semaphores start in a known state