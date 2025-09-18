# PGSemaphoreTryLock

## Location
src/backend/port/posix_sema.c: 365 - 388

## Overview
Attempts to lock a PostgreSQL semaphore without blocking, returning immediately with success or failure status.

## Definition
```c
bool PGSemaphoreTryLock(PGSemaphore sema);
```

## Detailed Description
PGSemaphoreTryLock provides a non-blocking attempt to acquire a PostgreSQL semaphore using the POSIX sem_trywait() function. Unlike PGSemaphoreLock(), this function never blocks the calling process. If the semaphore count is greater than zero, it decrements the count and returns true immediately. If the semaphore count is zero (meaning it's already locked), the function returns false without waiting.

The function handles signal interruptions (EINTR) by retrying the operation, ensuring that genuine interrupts don't cause false failures. It distinguishes between expected failure conditions (EAGAIN/EDEADLK indicating the semaphore is unavailable) and genuine error conditions, which result in fatal errors.

## Parameters / Member Variables
- `sema`: The PGSemaphore to attempt to lock

## Dependencies
- Functions called/Symbols referenced:
  - sem_trywait (POSIX semaphore try-wait function)
  - PG_SEM_REF (macro for getting semaphore reference)
  - elog (error reporting with FATAL level)
  - errno constants: EINTR, EAGAIN, EDEADLK
- Called from (representative examples):
  - [PGSemaphoreReset](PGSemaphoreReset.md) (used in reset implementation to drain semaphore counts)
  - [tas_sema](../t/tas_sema.md) (test-and-set semaphore operations for spinlocks)

## Notes and Other Information
- This is a non-blocking operation that returns immediately regardless of semaphore availability
- Returns true if the semaphore was successfully acquired, false if it was already locked
- Automatically handles signal interruptions (EINTR) by retrying the operation
- Uses the same underlying mechanism as PGSemaphoreReset for counting down semaphore values
- Essential for implementing lock-free algorithms and avoiding deadlocks in certain scenarios
- EAGAIN and EDEADLK errno values indicate normal failure (semaphore unavailable), not errors
- Any other errno values result in a FATAL error that terminates the process
- Commonly used in spinlock implementations and other performance-critical synchronization code
- Part of PostgreSQL's platform-independent semaphore API for non-blocking synchronization