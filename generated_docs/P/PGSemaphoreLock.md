# PGSemaphoreLock

## Location
[src/backend/port/posix_sema.c:320-339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/posix_sema.c#L320-L339)

## Overview
Locks a PostgreSQL semaphore by decrementing its count, blocking the calling process if the count would become negative.

## Definition
```c
void PGSemaphoreLock(PGSemaphore sema);
```

## Detailed Description
PGSemaphoreLock implements a blocking wait operation on a PostgreSQL semaphore. It uses the POSIX sem_wait() function to decrement the semaphore's count. If the semaphore count is greater than zero, the operation succeeds immediately and the count is decremented. If the count is zero, the calling process blocks until another process calls PGSemaphoreUnlock() to increment the count.

The function includes proper interrupt handling by retrying the sem_wait() call when interrupted by a signal (EINTR). This ensures that the lock operation completes successfully even in the presence of signal interruptions. Any other error conditions result in a fatal error.

## Parameters / Member Variables
- `sema`: The PGSemaphore to lock (acquire)

## Dependencies
- Functions called/Symbols referenced:
  - sem_wait (POSIX semaphore wait function)
  - PG_SEM_REF (macro for getting semaphore reference)
  - elog (error reporting with FATAL level)
  - errno constant: EINTR
- Called from (representative examples):
  - [TransactionGroupUpdateXidStatus](../T/TransactionGroupUpdateXidStatus.md) (transaction status updates)
  - [ProcArrayGroupClearXid](ProcArrayGroupClearXid.md) (process array management)
  - [LWLockDequeueSelf](../L/LWLockDequeueSelf.md) (lightweight lock management)
  - [LWLockAcquire](../L/LWLockAcquire.md) (lightweight lock acquisition)
  - [LWLockAcquireOrWait](../L/LWLockAcquireOrWait.md) (conditional lightweight lock acquisition)
  - [LWLockWaitForVar](../L/LWLockWaitForVar.md) (lightweight lock variable waiting)

## Notes and Other Information
- This is a blocking operation - the calling process will wait indefinitely until the semaphore becomes available
- The function automatically handles signal interruptions (EINTR) by retrying the operation
- Used extensively in PostgreSQL's lightweight locking system and transaction management
- The semaphore count is atomically decremented when the lock is acquired
- Any unexpected errors (other than EINTR) result in a FATAL error that terminates the process
- Part of PostgreSQL's platform-independent semaphore API for process synchronization