# PosixSemaphoreCreate

## Location
src/backend/port/posix_sema.c: 86 - 135

## Overview
PosixSemaphoreCreate is a static internal function that creates a new POSIX named semaphore with a unique name, handling collisions automatically and ensuring the semaphore is immediately unlinked for security.

## Definition


## Detailed Description
This function implements the core semaphore creation logic for PostgreSQL's POSIX semaphore implementation. It generates unique semaphore names using an incrementing key and attempts to create named semaphores with exclusive access. The function includes robust error handling and automatic retry logic for name collisions.

Key behaviors:
- Uses a global counter (nextSemKey) to generate unique semaphore names in the format "/pgsql-{key}"
- Creates semaphores with O_CREAT | O_EXCL flags to ensure exclusive creation
- Immediately unlinks the semaphore after creation to prevent external access and ensure cleanup on crash
- Implements retry logic for recoverable errors (EEXIST, EACCES, EINTR)
- Terminates the process with FATAL error for unrecoverable failures

## Parameters / Member Variables
This function takes no parameters and returns:
- : Pointer to the created POSIX semaphore, or terminates process on failure

## Dependencies
- Functions called/Symbols referenced:
  - sem_open (POSIX semaphore creation)
  - sem_unlink (POSIX semaphore unlinking)
  - snprintf (string formatting)
  - elog (PostgreSQL logging)
  - IPCProtection (PostgreSQL IPC permission settings)
  - nextSemKey (global semaphore key counter)

- Called from:
  - PGSemaphoreCreate (PostgreSQL's public semaphore creation interface)

## Notes and Other Information
- The function is static and internal to the POSIX semaphore implementation
- Semaphores are created with initial value 1 (unlocked state)
- The immediate unlinking ensures semaphores cannot be accessed by external processes
- Error handling distinguishes between recoverable collisions and fatal system errors
- Uses conditional compilation (#ifdef SEM_FAILED) for portability across different POSIX implementations