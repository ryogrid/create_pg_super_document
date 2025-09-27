# PosixSemaphoreCreate

## Location
[src/backend/port/posix_sema.c:86-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/posix_sema.c#L86-L135)

## Overview
PosixSemaphoreCreate is a static internal function that creates a new POSIX named semaphore with a unique name, handling collisions automatically and ensuring the semaphore is immediately unlinked for security.

## Definition

```c
static sem_t *
PosixSemaphoreCreate(void)
```
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
- `sem_t *`: Pointer to the created POSIX semaphore, or terminates process on failure

## Dependencies
- Functions called/Symbols referenced:
  - sem_open (POSIX semaphore creation)
  - sem_unlink (POSIX semaphore unlinking)
  - snprintf (string formatting)
  - elog (PostgreSQL logging)
  - IPCProtection (PostgreSQL IPC permission settings)
  - nextSemKey (global semaphore key counter)

- Called from:
  - [PGSemaphoreCreate](PGSemaphoreCreate.md) (PostgreSQL's public semaphore creation interface)

## Notes and Other Information
- The function is static and internal to the POSIX semaphore implementation
- Semaphores are created with initial value 1 (unlocked state)
- The immediate unlinking ensures semaphores cannot be accessed by external processes
- Error handling distinguishes between recoverable collisions and fatal system errors
- Uses conditional compilation (#ifdef SEM_FAILED) for portability across different POSIX implementations

## Simplified Source

```c
// Simplified version of PosixSemaphoreCreate
static sem_t *PosixSemaphoreCreate(void) {
    int semKey;
    char semname[64];
    sem_t *mySem;

    // Loop until we successfully create a unique semaphore
    for (;;) {
        // Generate unique semaphore name
        semKey = nextSemKey++;
        snprintf(semname, sizeof(semname), "/pgsql-%d", semKey);

        // Attempt to create semaphore with exclusive access
        mySem = sem_open(semname, O_CREAT | O_EXCL,
                         (mode_t) IPCProtection, (unsigned) 1);

        // Check for successful creation
#ifdef SEM_FAILED
        if (mySem != (sem_t *) SEM_FAILED)
            break;
#else
        if (mySem != (sem_t *) (-1))
            break;
#endif

        // Retry for recoverable errors (name collision, access, interrupt)
        if (errno == EEXIST || errno == EACCES || errno == EINTR)
            continue;

        // Fatal error for unrecoverable problems
        elog(FATAL, "sem_open(\"%s\") failed: %m", semname);
    }

    // Immediately unlink for security and automatic cleanup
    sem_unlink(semname);

    return mySem;
}
```

Key simplifications made:
- Added clear comments explaining the unique name generation and retry logic
- Preserved essential error handling and recovery mechanisms
- Maintained security feature of immediate unlinking
- Focused on the core algorithm: create unique semaphore with collision handling
- Kept platform compatibility through conditional compilation