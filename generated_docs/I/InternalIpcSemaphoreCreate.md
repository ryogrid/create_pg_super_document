# InternalIpcSemaphoreCreate

## Location
src/backend/port/sysv_sema.c: 103 - 162

## Overview
InternalIpcSemaphoreCreate is a static function that attempts to create a new System V IPC semaphore set with a specified key, designed to fail gracefully if the semaphore set already exists.

## Definition


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
  - errmsg
  - errdetail
  - errhint
- Constants referenced:
  - IPC_CREAT
  - IPC_EXCL
  - IPCProtection
  - EEXIST, EACCES, EINVAL, EIDRM, ENOSPC (errno values)
- Called from (representative examples):
  - IpcSemaphoreCreate

## Notes and Other Information
- This is a static function, only accessible within the sysv_sema.c file
- The function provides detailed error messages for ENOSPC errors, explaining that this doesn't mean disk space is exhausted but rather indicates system semaphore limits (SEMMNI or SEMMNS) have been reached
- The retry_ok parameter allows callers to implement retry loops while distinguishing between retryable and fatal errors
- Platform-specific handling for EIDRM error code (conditionally compiled)
- Returns -1 on retryable errors, positive semaphore ID on success, never returns on fatal errors (calls ereport with FATAL level)