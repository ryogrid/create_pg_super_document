# InternalIpcSemaphoreCreate

## Location
[src/backend/port/sysv_sema.c:103-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/sysv_sema.c#L103-L162)

## Overview
InternalIpcSemaphoreCreate is a static function that attempts to create a new System V IPC semaphore set with a specified key, designed to fail gracefully if the semaphore set already exists.

## Definition

```c
struction but
		 * not gone yet.
		 *
		 * EINVAL is the key reason why we need the caller-level loop limit,
		 * as it can also mean that the platform's SEMMSL is less than
		 * numSems, and that condition can't be fixed by trying another key.
		 */
		if (retry_ok &&
			(saved_errno == EEXIST
			 || saved_errno == EACCES
			 || saved_errno == EINVAL
#ifdef EIDRM
			 || saved_errno == EIDRM
#endif
			 ))
			return -1;
```
## Detailed Description
This function serves as a low-level wrapper around the system  call with IPC_CREAT and IPC_EXCL flags to create a new semaphore set. It implements careful error handling to distinguish between recoverable errors (like collisions with existing semaphore sets) and fatal system errors. The function uses IPC_EXCL to ensure it only succeeds when creating a genuinely new semaphore set, preventing accidental reuse of existing semaphores.

The function includes sophisticated error classification logic that recognizes various errno values that might indicate a collision with an existing semaphore set (EEXIST, EACCES, EINVAL, EIDRM). When such collisions occur and the caller indicates retries are acceptable, the function returns -1 to allow the caller to try with a different key. For other types of errors, or when retries are not acceptable, the function reports a FATAL error with detailed diagnostic information.

## Parameters / Member Variables
- : The System V IPC key to use for the semaphore set creation
- : The number of semaphores to include in the set
- : Boolean flag indicating whether the caller can handle collision errors and retry with different parameters

## Dependencies
- Functions called/Symbols referenced:
  - semget (system call)
  - ereport
  - [errmsg](../e/errmsg.md)
  - [errdetail](../e/errdetail.md)
  - [errhint](../e/errhint.md)
- Constants referenced:
  - IPC_CREAT
  - IPC_EXCL
  - IPCProtection
  - EEXIST, EACCES, EINVAL, EIDRM, ENOSPC (errno values)
- Called from (representative examples):
  - [IpcSemaphoreCreate](IpcSemaphoreCreate.md)

## Notes and Other Information
- This is a static function, only accessible within the sysv_sema.c file
- The function provides detailed error messages for ENOSPC errors, explaining that this doesn't mean disk space is exhausted but rather indicates system semaphore limits (SEMMNI or SEMMNS) have been reached
- The retry_ok parameter allows callers to implement retry loops while distinguishing between retryable and fatal errors
- Platform-specific handling for EIDRM error code (conditionally compiled)
- Returns -1 on retryable errors, positive semaphore ID on success, never returns on fatal errors (calls ereport with FATAL level)

## Simplified Source

```c
static IpcSemaphoreId InternalIpcSemaphoreCreate(IpcSemaphoreKey semKey, int numSems, bool retry_ok) {
    int semId;

    // Try to create new semaphore set with exclusive access
    semId = semget(semKey, numSems, IPC_CREAT | IPC_EXCL | IPCProtection);

    if (semId < 0) {
        int saved_errno = errno;

        // Handle retryable errors if caller allows retries
        if (retry_ok &&
            (saved_errno == EEXIST ||   // Already exists
             saved_errno == EACCES ||   // Permission denied
             saved_errno == EINVAL ||   // Invalid parameter/too few sems
             saved_errno == EIDRM)) {   // Being destroyed
            return -1;  // Let caller retry with different key
        }

        // Fatal error - report with detailed diagnostics
        ereport(FATAL,
            (errmsg("could not create semaphores: %m"),
             errdetail("Failed system call was semget(%lu, %d, 0%o).",
                       (unsigned long) semKey, numSems,
                       IPC_CREAT | IPC_EXCL | IPCProtection),
             // Special hint for ENOSPC about system limits
             (saved_errno == ENOSPC) ?
             errhint("System semaphore limits exceeded. Increase SEMMNI/SEMMNS "
                     "or reduce max_connections.") : 0));
    }

    return semId;  // Success
}
```