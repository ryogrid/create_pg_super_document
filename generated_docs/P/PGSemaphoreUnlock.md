# PGSemaphoreUnlock

## Location
[src/backend/port/posix_sema.c:340-364](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/posix_sema.c#L340-L364)

## Overview
Unlocks a PostgreSQL semaphore by incrementing its count, potentially waking up processes blocked on PGSemaphoreLock calls.

## Definition
```c
void PGSemaphoreUnlock(PGSemaphore sema);
```

## Detailed Description
PGSemaphoreUnlock implements the release operation for PostgreSQL semaphores using the POSIX sem_post() function. It atomically increments the semaphore's count, making the semaphore available for other processes to acquire. If any processes are blocked waiting on PGSemaphoreLock() calls for this semaphore, one of them will be awakened and allowed to proceed.

The function includes robust interrupt handling by retrying the sem_post() call if interrupted by a signal (EINTR). This ensures the unlock operation completes successfully even when signals are delivered during the operation. The function treats any other error conditions as fatal.

## Parameters / Member Variables
- `sema`: The PGSemaphore to unlock (release)

## Dependencies
- Functions called/Symbols referenced:
  - sem_post (POSIX semaphore post function)
  - PG_SEM_REF (macro for getting semaphore reference)
  - elog (error reporting with FATAL level)
  - errno constant: EINTR
- Called from (representative examples):
  - [TransactionGroupUpdateXidStatus](../T/TransactionGroupUpdateXidStatus.md) (transaction status updates)
  - [IpcSemaphoreCreate](../I/IpcSemaphoreCreate.md) (IPC semaphore initialization)
  - [ProcArrayGroupClearXid](ProcArrayGroupClearXid.md) (process array management)
  - [LWLockWakeup](../L/LWLockWakeup.md) (lightweight lock wakeup operations)
  - [LWLockDequeueSelf](../L/LWLockDequeueSelf.md) (lightweight lock queue management)
  - [LWLockAcquire](../L/LWLockAcquire.md) (lightweight lock acquisition completion)
  - [LWLockAcquireOrWait](../L/LWLockAcquireOrWait.md) (conditional lightweight lock operations)
  - [LWLockWaitForVar](../L/LWLockWaitForVar.md) (lightweight lock variable operations)
  - [LWLockUpdateVar](../L/LWLockUpdateVar.md) (lightweight lock variable updates)
  - [s_unlock_sema](../s/s_unlock_sema.md) (spinlock semaphore operations)

## Notes and Other Information
- This operation never blocks - it always completes immediately (barring fatal errors)
- Automatically handles signal interruptions (EINTR) by retrying the operation
- Used extensively throughout PostgreSQL's concurrency control mechanisms
- The semaphore count is atomically incremented, making it available for waiting processes
- If processes are blocked on PGSemaphoreLock(), exactly one will be awakened by this operation
- Any unexpected errors (other than EINTR) result in a FATAL error that terminates the process
- Critical for releasing resources and coordinating between PostgreSQL processes
- Part of PostgreSQL's platform-independent semaphore API for process synchronization

## Simplified Source

```c
// Simplified version of PGSemaphoreUnlock
void PGSemaphoreUnlock(PGSemaphore sema) {
    int errStatus;

    // Keep trying to release the semaphore until successful
    do {
        errStatus = sem_post(PG_SEM_REF(sema));
    } while (errStatus < 0 && errno == EINTR);  // Retry on signal interruption

    // Fatal error if semaphore operation failed
    if (errStatus < 0) {
        elog(FATAL, "sem_post failed: %m");
    }
}
```

Key simplifications made:
- Preserved the core POSIX semaphore release logic
- Maintained the interrupt handling loop for EINTR
- Removed verbose comments about unlikely EINTR scenarios
- Kept the fatal error handling for robustness
- Added clear comments explaining the operation flow